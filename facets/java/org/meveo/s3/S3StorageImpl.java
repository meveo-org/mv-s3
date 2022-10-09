package org.meveo.s3;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.crm.custom.CustomFieldStorageTypeEnum;
import org.meveo.model.crm.custom.CustomFieldTypeEnum;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.model.storage.IStorageConfiguration;
import org.meveo.model.storage.Repository;
import org.meveo.persistence.PersistenceActionResult;
import org.meveo.persistence.StorageImpl;
import org.meveo.persistence.StorageQuery;
import org.meveo.service.crm.impl.CustomFieldInstanceService;
import org.meveo.service.crm.impl.CustomFieldTemplateService;
import org.meveo.service.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3StorageImpl extends Script implements StorageImpl {

	private static Logger LOG = LoggerFactory.getLogger(S3StorageImpl.class);
	
	private Map<String, AmazonS3> clients = new ConcurrentHashMap<>();

	private static DBStorageType storageType() {
		DBStorageType dbStorageType = new DBStorageType();
		dbStorageType.setCode("S3");
		return dbStorageType;
	}

	@Override
	public boolean exists(IStorageConfiguration repository, CustomEntityTemplate cet, String uuid) {
		return false;
	}

	@Override
	public String findEntityIdByValues(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei) {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> findById(IStorageConfiguration conf, CustomEntityTemplate cet, String uuid,
			Map<String, CustomFieldTemplate> cfts, Collection<String> fetchFields, boolean withEntityReferences) {
			
		Map<String, Object> result = new HashMap<>();
		
		cfts.values()
			.stream()
			.filter(cft -> cft.getFieldType() == CustomFieldTypeEnum.BINARY)
			.filter(cft -> fetchFields.contains(cft.getCode()))
			.forEach(cft -> {
				AmazonS3 client = beginTransaction(conf, 0);
				String bucketName = S3Utils.getS3BucketName(conf, cet, cft);
				
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
						.withBucketName(bucketName)
		                .withPrefix(uuid + "/")
		                .withDelimiter("/");
				
				ObjectListing objectsListing = client.listObjects(listObjectsRequest);
				List<S3ObjectSummary> summaries = objectsListing.getObjectSummaries();
				
				summaries.forEach(file -> {
					if (cft.getStorageType() == CustomFieldStorageTypeEnum.SINGLE) {
						result.put(cft.getCode(), client.getObject(bucketName, file.getKey()).getObjectContent());
					} else {
						List<Object> fileList = (List<Object>) result.computeIfAbsent(cft.getCode(), code -> new ArrayList<>());
						fileList.add(client.getObject(bucketName, file.getKey()).getObjectContent());
					}
				});
			});
		
		return result;
	}

	@Override
	public List<Map<String, Object>> find(StorageQuery query) throws EntityDoesNotExistsException {
		// Never eagerly load files
		return new ArrayList<>();
	}


	@Override
	public PersistenceActionResult createOrUpdate(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei,
			Map<String, CustomFieldTemplate> customFieldTemplates, String foundUuid) throws BusinessException {
		
		customFieldTemplates.values()
			.stream()
			.filter(cft -> cft.getFieldType() == CustomFieldTypeEnum.BINARY)
			.forEach(cft -> {
				AmazonS3 client = beginTransaction(conf, 0);
				String bucketName = S3Utils.getS3BucketName(conf, cei.getCet(), cft);
				
				File fileValue = cei.getCfValues().getCfValue(cft.getCode()).getFileValue();
				List<File> filesValue = cei.getCfValues().getCfValue(cft.getCode()).getListValue();
				
				List<File> files = new ArrayList<>();
				if (fileValue != null) {
					files.add(fileValue);
				} else if (filesValue != null) {
					filesValue.addAll(filesValue);
				}
				
				files.forEach(file -> {
					client.putObject(bucketName, foundUuid + "/" + file.getName(), file);
				});
				
			});
		
		return new PersistenceActionResult(foundUuid);
	}

	@Override
	public PersistenceActionResult addCRTByUuids(IStorageConfiguration repository, CustomRelationshipTemplate crt,
			Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		return null;
	}

	@Override
	public void update(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei) throws BusinessException {
		//TODO
	}

	@Override
	public void setBinaries(IStorageConfiguration conf, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid, List<File> binaries) throws BusinessException {
	}

	@Override
	public void remove(IStorageConfiguration repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		//TODO
	}

	@Override
	public Integer count(IStorageConfiguration repository, CustomEntityTemplate cet, PaginationConfiguration paginationConfiguration) {
		return 0;
	}

	@Override
	public void cetCreated(CustomEntityTemplate cet) {
		
	}

	@Override
	public void removeCet(CustomEntityTemplate cet) {
		
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		//NOOP
	}

	@Override
	public void cftCreated(CustomModelObject template, CustomFieldTemplate cft) {
		if (!cft.getFieldType().equals(CustomFieldTypeEnum.BINARY)) {
			return;
		}
		
		for (var repository : template.getRepositories()) {
			repository.getStorageConfigurations(storageType())
				.forEach(conf -> {
					// Check if bucket exists
					AmazonS3 client = beginTransaction(conf, 0);
					
					String bucketName = S3Utils.getS3BucketName(conf, template, cft);
					
					if (client.doesBucketExistV2(bucketName)) {
						List<String> accessibleBuckets = client.listBuckets()
								.stream()
								.map(Bucket::getName)
								.collect(Collectors.toList());
						if (accessibleBuckets.contains(bucketName)) {
							//TODO: Raise error
						}
					}
				});
		}

	}

	@Override
	public void cetUpdated(CustomEntityTemplate oldCet, CustomEntityTemplate cet) {
		//NOOP
	}

	@Override
	public void crtUpdated(CustomRelationshipTemplate cet) throws BusinessException {
		//NOOP 
	}

	@Override
	public void cftUpdated(CustomModelObject template, CustomFieldTemplate oldCft, CustomFieldTemplate cft) {
		cftCreated(template, cft);
	}

	@Override
	public void removeCft(CustomModelObject template, CustomFieldTemplate cft) {
		//NOOP
	}

	@Override
	public void removeCrt(CustomRelationshipTemplate crt) {
		//NOOP
	}

	@Override
	public void init() {
		//NOOP
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T beginTransaction(IStorageConfiguration repository, int stackedCalls) {
		return (T) clients.computeIfAbsent(repository.getCode(), code -> {
			String s3Host = repository.getHostname();
			String s3AccessKey = repository.getCredential().getUsername();
			String s3SecretKey = repository.getCredential().getPassword();
			String preferredRegion =  repository.getCfValues().getCfValue("preferredRegion").getStringValue();
			
			AWSCredentials credentials = new BasicAWSCredentials(
				s3AccessKey, 
				s3SecretKey
			);
			
			return AmazonS3ClientBuilder
					  .standard()
					  .withEndpointConfiguration(new EndpointConfiguration(s3Host, preferredRegion))
					  .withCredentials(new AWSStaticCredentialsProvider(credentials))
					  .withRegion(preferredRegion)
					  .build();
		});
	}

	@Override
	public void commitTransaction(IStorageConfiguration repository) {
		// NOOP
	}

	@Override
	public void rollbackTransaction(int stackedCalls) {
		// NOOP
	}

	@Override
	public void destroy() {
		//TODO
	}

}

package org.meveo.s3;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.crm.custom.CustomFieldTypeEnum;
import org.meveo.model.customEntities.BinaryProvider;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.module.MeveoModule;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.model.storage.IStorageConfiguration;
import org.meveo.model.storage.Repository;
import org.meveo.persistence.PersistenceActionResult;
import org.meveo.persistence.StorageImpl;
import org.meveo.persistence.StorageQuery;
import org.meveo.service.crm.impl.CustomFieldTemplateService;
import org.meveo.service.custom.CustomEntityTemplateService;
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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3StorageImpl extends Script implements StorageImpl {

	private static Logger LOG = LoggerFactory.getLogger(S3StorageImpl.class);
	
	private CustomEntityTemplateService cetService = getCDIBean(CustomEntityTemplateService.class);
	private CustomFieldTemplateService cftService = getCDIBean(CustomFieldTemplateService.class);
	
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
		MeveoModule module = cetService.findModuleOf(cet);
		
		cfts.values()
			.stream()
			.filter(cft -> cft.getFieldType() == CustomFieldTypeEnum.BINARY)
			.filter(cft -> fetchFields.contains(cft.getCode()))
			.forEach(cft -> {
				AmazonS3 client = beginTransaction(conf, 0);
				String bucketName = S3Utils.getS3BucketName(conf, module, cet, cft);
				
				LOG.info("Retrieving binaries in bucket {}", bucketName);
				
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
						.withBucketName(bucketName)
		                .withPrefix(uuid + "/")
		                .withDelimiter("/");
				
				ObjectListing objectsListing = client.listObjects(listObjectsRequest);
				List<S3ObjectSummary> summaries = objectsListing.getObjectSummaries();
				
				summaries.forEach(file -> {
					List<BinaryProvider> fileList = (List<BinaryProvider>) result.computeIfAbsent(cft.getCode(), code -> new ArrayList<>());
					fileList.add(
						new BinaryProvider(
							S3Utils.getFileName(uuid, file.getKey()), 
							() -> client.getObject(bucketName, file.getKey()).getObjectContent(),
							false
						)
					);
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
		
		MeveoModule module = cetService.findModuleOf(cei.getCet());
		
		final String uuid = StringUtils.isEmpty(foundUuid) ? cei.getUuid() : foundUuid;
		
		customFieldTemplates.values()
			.stream()
			.filter(cft -> cft.getFieldType() == CustomFieldTypeEnum.BINARY)
			.forEach(cft -> {
				AmazonS3 client = beginTransaction(conf, 0);
				String bucketName = S3Utils.getS3BucketName(conf, module, cei.getCet(), cft);
				
				if (client.doesBucketExistV2(bucketName)) {
					var cfValue = cei.getCfValues().getCfValue(cft.getCode());
					Set<String> fileNames = cfValue.getFileNames();
					
					cfValue.getBinaries().forEach(binary -> {
						LOG.info("Uploading file {} to bucket {} for uuid {}", binary.getFileName(), bucketName, uuid);
						client.putObject(bucketName, uuid + "/" + binary.getFileName(), binary.getBinary(), new ObjectMetadata());
					});
					
				// Remove objects not present on the updated list
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
						.withBucketName(bucketName)
		                .withPrefix(uuid + "/")
		                .withDelimiter("/");
				
				
				ObjectListing objectsListing = client.listObjects(listObjectsRequest);
				List<S3ObjectSummary> summaries = objectsListing.getObjectSummaries();
				LOG.debug("Files summary : {}", summaries);
				
				summaries.stream()
					.filter(summary -> !fileNames.contains(S3Utils.getFileName(uuid, summary.getKey())))
					.forEach(summary -> {
						LOG.debug("Delete file {} from bucket", summary.getKey());
						client.deleteObject(bucketName, summary.getKey());
					});
				
			} else {
				LOG.warn("Bucket {} does not exists", bucketName);
			}
		});
		
		return new PersistenceActionResult(uuid);
	}

	@Override
	public PersistenceActionResult addCRTByUuids(IStorageConfiguration repository, CustomRelationshipTemplate crt,
			Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		return null;
	}

	@Override
	public void update(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei) throws BusinessException {
		createOrUpdate(repository, conf, cei, cei.getFieldTemplates(), cei.getUuid());
	}

	@Override
	public void setBinaries(IStorageConfiguration conf, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid, List<File> binaries) throws BusinessException {
	}

	@Override
	public void remove(IStorageConfiguration conf, CustomEntityTemplate cet, String uuid) throws BusinessException {
		MeveoModule module = cetService.findModuleOf(cet);
		
		cftService.findByAppliesTo(cet.getAppliesTo())
			.values()
			.stream()
			.filter(cft -> cft.getFieldType() == CustomFieldTypeEnum.BINARY)
			.forEach(cft -> {
				AmazonS3 client = beginTransaction(conf, 0);
				String bucketName = S3Utils.getS3BucketName(conf, module, cet, cft);
				
				if (client.doesBucketExistV2(bucketName)) {
					// Remove objects not present on the updated list
					ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
							.withBucketName(bucketName)
			                .withPrefix(uuid + "/")
			                .withDelimiter("/");
					
					
					String[] keys = client.listObjects(listObjectsRequest)
							.getObjectSummaries()
							.stream()
							.map(S3ObjectSummary::getKey)
							.toArray(String[]::new);
					
					DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName)
							.withKeys(keys);
					
					DeleteObjectsResult results = client.deleteObjects(deleteRequest);
					LOG.debug("Deleted objects {}", results.getDeletedObjects());
					
				} else {
					LOG.warn("Bucket {} does not exists", bucketName);
				}
			});
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
		
        CustomEntityTemplate cet = cetService.findByCode(CustomEntityTemplate.getCodeFromAppliesTo(cft.getAppliesTo()));
        MeveoModule relatedModule = cetService.findModuleOf(cet);
		
		for (var repository : template.getRepositories()) {
			repository.getStorageConfigurations(storageType())
				.forEach(conf -> {
					// Check if bucket exists
					AmazonS3 client = beginTransaction(conf, 0);
					
					String bucketName = S3Utils.getS3BucketName(conf, relatedModule, template, cft);
					
					if (client.doesBucketExistV2(bucketName)) {
						List<String> accessibleBuckets = client.listBuckets()
								.stream()
								.map(Bucket::getName)
								.collect(Collectors.toList());
						if (!accessibleBuckets.contains(bucketName)) {
							//TODO: Raise error
						}
					} else {
						client.createBucket(bucketName);
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

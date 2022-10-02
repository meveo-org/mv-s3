package org.meveo.s3;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.crm.custom.CustomFieldTypeEnum;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.model.persistence.JacksonUtil;
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

public class S3StorageImpl extends Script implements StorageImpl {

	private CustomFieldTemplateService cftService = getCDIBean(CustomFieldTemplateService.class);
	
	private CustomFieldInstanceService cfiService = getCDIBean(CustomFieldInstanceService.class);

	private static Logger LOG = LoggerFactory.getLogger(S3StorageImpl.class);

	private static DBStorageType storageType() {
		DBStorageType dbStorageType = new DBStorageType();
		dbStorageType.setCode("S3");
		return dbStorageType;
	}


	@Override
	public boolean exists(IStorageConfiguration repository, CustomEntityTemplate cet, String uuid) {
		//TODO
		return false;
	}

	private String buildSearchRequest(StorageQuery query, Map<String, CustomFieldTemplate> fields) {
		var json = JacksonUtil.OBJECT_MAPPER.createObjectNode();

		var queries = json.putObject("query")
			.putObject("bool")
			.putArray("must");

		query.getFilters().forEach((filterKey, filterValue) -> {
			if (!filterKey.equals("uuid") && filterValue != null) {
				var cft = fields.get(filterKey);

				if (cft.getFieldType() == CustomFieldTypeEnum.STRING) {
					queries.addObject()
						.putObject("wildcard")
						.put(filterKey.toLowerCase(), String.valueOf(filterValue));
				} else {
					queries.addObject()
						.putObject("match")
						.putObject(filterKey.toLowerCase())
						.put("query", String.valueOf(filterValue))
						.put("fuzziness", "AUTO");
				}

			}
		});
		
		return json.toString();
	}

	@Override
	public String findEntityIdByValues(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei) {
		StorageQuery query = new StorageQuery();
		query.setCet(cei.getCet());
		query.setStorageConfiguration(conf);
		query.setFilters(cei.getCfValuesAsValues(storageType(), cei.getFieldTemplates().values(), true));

		try {
			var result = this.find(query);
			if (result.size() == 1) {
				return (String) result.get(0).get("uuid");
			} else if (result.size() > 1) {
				throw new PersistenceException("Many possible entity for values " + query.getFilters().toString());
			}

		} catch (EntityDoesNotExistsException e) {
			throw new PersistenceException("Template does not exists", e);
		}

		return null;
	}

	@Override
	public Map<String, Object> findById(IStorageConfiguration repository, CustomEntityTemplate cet, String uuid,
			Map<String, CustomFieldTemplate> cfts, Collection<String> fetchFields, boolean withEntityReferences) {
			
		//TODO
		return null;
	}

	@Override
	public List<Map<String, Object>> find(StorageQuery query) throws EntityDoesNotExistsException {
		//TODO
		return null;
	}


	@Override
	public PersistenceActionResult createOrUpdate(Repository repository, IStorageConfiguration conf, CustomEntityInstance cei,
			Map<String, CustomFieldTemplate> customFieldTemplates, String foundUuid) throws BusinessException {
		
		//TODO
		return null;
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
	public void setBinaries(IStorageConfiguration repository, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid,
			List<File> binaries) throws BusinessException {

	}

	@Override
	public void remove(IStorageConfiguration repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		//TODO
	}

	@Override
	public Integer count(IStorageConfiguration repository, CustomEntityTemplate cet, PaginationConfiguration paginationConfiguration) {
		final Map<String, Object> filters = paginationConfiguration == null ? null : paginationConfiguration.getFilters();

		return 0;
	}

	@Override
	public void cetCreated(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			repository.getStorageConfigurations(storageType())
			.forEach(conf -> {
				//TODO
			});
		}
	}

	@Override
	public void removeCet(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			repository.getStorageConfigurations(storageType())
				.forEach(conf -> {
					//TODO
				});

		}
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		//NOOP
	}

	@Override
	public void cftCreated(CustomModelObject template, CustomFieldTemplate cft) {
		for (var repository : template.getRepositories()) {
			repository.getStorageConfigurations(storageType())
				.forEach(conf -> {
					//TODO
				});
		}

	}

	@Override
	public void cetUpdated(CustomEntityTemplate oldCet, CustomEntityTemplate cet) {
		//NOOP - TODO later
	}

	@Override
	public void crtUpdated(CustomRelationshipTemplate cet) throws BusinessException {
		//NOOP 
	}

	@Override
	public void cftUpdated(CustomModelObject template, CustomFieldTemplate oldCft, CustomFieldTemplate cft) {
		//NOOP - TODO later
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
		return null;
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

	private static Map<String, Object> getPropertyFromCft(CustomFieldTemplate cft) {
		Map<String, Object> property = new HashMap<>();

		switch (cft.getFieldType()) {
			case LONG:
			case LONG_TEXT:
			case TEXT_AREA:
				// "text"
				property.put("type", "text");
				break;
			case STRING:
				// search_as_you_type
				property.put("type", "search_as_you_type");
				break;
			default:
				break;
		}

		return property;
	}

}

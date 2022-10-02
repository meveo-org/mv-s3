package org.meveo.s3;

import java.util.Map;
import java.util.Set;

import org.meveo.admin.exception.BusinessException;
import org.meveo.model.crm.custom.CustomFieldTypeEnum;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.persistence.DBStorageTypeService;
import org.meveo.service.admin.impl.ModuleInstallationContext;
import org.meveo.service.script.ScriptInstanceService;
import org.meveo.service.script.module.ModuleScript;

public class S3InstallationScript extends ModuleScript {
	
    DBStorageTypeService dbStorageTypeService = getCDIBean(DBStorageTypeService.class);
    ScriptInstanceService scriptInstanceService = getCDIBean(ScriptInstanceService.class);
    ModuleInstallationContext installationContext = getCDIBean(ModuleInstallationContext.class);

    @Override
    public void execute(Map<String, Object> methodContext) throws BusinessException {
    	postInstallModule(methodContext);
    }
    
    @Override
    public void postInstallModule(Map<String, Object> methodContext) throws BusinessException {
        // Register new storage type
        DBStorageType s3DbStorageType = new DBStorageType();
        s3DbStorageType.setCode("S3");
        s3DbStorageType.setStorageImplScript(scriptInstanceService.findByCode("org.meveo.persistence.impl.S3StorageImpl"));
        s3DbStorageType.setSupportedFieldTypes(Set.of(CustomFieldTypeEnum.BINARY));
        dbStorageTypeService.create(s3DbStorageType);
    }
  
    @Override
    public void preUninstallModule(Map<String, Object> methodContext) throws BusinessException {
      DBStorageType elasticDbStorageType = dbStorageTypeService.find("S3");
      if (elasticDbStorageType != null) {
      	dbStorageTypeService.delete(elasticDbStorageType);
      }
    }
	
}
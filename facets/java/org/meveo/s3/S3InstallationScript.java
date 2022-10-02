package org.meveo.s3;

import java.util.Map;

import org.meveo.admin.exception.BusinessException;
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
    public void postInstallModule(Map<String, Object> methodContext) throws BusinessException {
        // Register new storage type
        DBStorageType elasticDbStorageType = new DBStorageType();
        elasticDbStorageType.setCode("S3");
        elasticDbStorageType.setStorageImplScript(scriptInstanceService.findByCode("org.meveo.persistence.impl.S3StorageImpl"));
        dbStorageTypeService.create(elasticDbStorageType);
    }
  
    @Override
    public void preUninstallModule(Map<String, Object> methodContext) throws BusinessException {
      DBStorageType elasticDbStorageType = dbStorageTypeService.find("S3");
      if (elasticDbStorageType != null) {
      	dbStorageTypeService.delete(elasticDbStorageType);
      }
    }
	
}
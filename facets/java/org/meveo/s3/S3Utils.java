/**
 * 
 */
package org.meveo.s3;

import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.module.MeveoModule;
import org.meveo.model.storage.IStorageConfiguration;

public class S3Utils {
	
	public static String getS3BucketName(IStorageConfiguration conf, MeveoModule module, CustomModelObject template, CustomFieldTemplate cft) {
		String orgName = conf.getCfValues().getCfValue("orgName").getStringValue();
		return new StringBuilder()
				.append(orgName.toLowerCase().trim().replaceAll("\\s", "-"))
				.append(".")
				.append(module.getCode().trim().toLowerCase().replaceAll("\\s", "-"))
				.append(".")
				.append(template.getCode().toLowerCase())
				.append(".")
				.append(cft.getCode().toLowerCase())
				.toString();
	}
	
	public static String getFileName(String uuid, String keyName) {
 		int indexOfFileName = keyName.indexOf(uuid) + uuid.length() + 1;
		return keyName.substring(indexOfFileName);
	}
}

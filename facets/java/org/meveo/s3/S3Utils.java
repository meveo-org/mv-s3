/**
 * 
 */
package org.meveo.s3;

import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.storage.IStorageConfiguration;

public class S3Utils {
	public static String getS3BucketName(IStorageConfiguration conf, CustomModelObject template, CustomFieldTemplate cft) {
		String orgName = conf.getCfValues().getCfValue("orgName").getStringValue();
		return new StringBuilder()
				.append(orgName.toLowerCase())
				.append(".")
				.append(template.getCode().toLowerCase())
				.append(".")
				.append(cft.getCode().toLowerCase())
				.toString();
	}
}

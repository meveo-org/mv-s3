/**
 * 
 */
package org.meveo.s3;

import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomModelObject;

/**
 * 
 * @author heros
 * @since 
 * @version
 */
public class S3Utils {
	public static String getS3BucketName(String orgName, CustomModelObject template, CustomFieldTemplate cft) {
		return new StringBuilder()
				.append(orgName.toLowerCase())
				.append(".")
				.append(template.getCode().toLowerCase())
				.append(".")
				.append(cft.getCode().toLowerCase())
				.toString();
	}
}

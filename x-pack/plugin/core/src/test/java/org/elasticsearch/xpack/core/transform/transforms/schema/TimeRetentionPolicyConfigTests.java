package org.elasticsearch.xpack.core.transform.transforms.schema;

import org.elasticsearch.test.AbstractSchemaValidationTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;

import static org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig;

public class TimeRetentionPolicyConfigTests extends AbstractSchemaValidationTestCase<TimeRetentionPolicyConfig> {

    @Override
    protected TimeRetentionPolicyConfig createTestInstance() {
        return randomTimeRetentionPolicyConfig();
    }

    @Override
    protected String getJsonSchemaFileName() {
        return "transform_time_retention_policy_config.schema.json";
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.is;

public class PerPartitionCategorizationConfigTests extends AbstractSerializingTestCase<PerPartitionCategorizationConfig> {

    public void testConstructorDefaults() {
        assertThat(new PerPartitionCategorizationConfig().isEnabled(), is(false));
        assertThat(new PerPartitionCategorizationConfig().isStopOnWarn(), is(false));
    }

    public void testValidation() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new PerPartitionCategorizationConfig(false, true));

        assertThat(e.getMessage(), is("stop_on_warn cannot be true in per_partition_categorization when enabled is false"));
    }

    @Override
    protected PerPartitionCategorizationConfig createTestInstance() {
        boolean enabled = randomBoolean();
        return new PerPartitionCategorizationConfig(enabled, randomBoolean() ? null : enabled && randomBoolean());
    }

    @Override
    protected Writeable.Reader<PerPartitionCategorizationConfig> instanceReader() {
        return PerPartitionCategorizationConfig::new;
    }

    @Override
    protected PerPartitionCategorizationConfig doParseInstance(XContentParser parser) {
        return PerPartitionCategorizationConfig.STRICT_PARSER.apply(parser, null);
    }
}

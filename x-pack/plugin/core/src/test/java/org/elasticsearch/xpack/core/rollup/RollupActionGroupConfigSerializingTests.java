/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupActionGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupActionGroupConfigSerializingTests extends AbstractSerializingTestCase<RollupActionGroupConfig> {

    @Override
    protected RollupActionGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupActionGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupActionGroupConfig> instanceReader() {
        return RollupActionGroupConfig::new;
    }

    @Override
    protected RollupActionGroupConfig createTestInstance() {
        return randomRollupActionGroupConfig(random());
    }

    public void testValidatesDateHistogramConfig() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("date_field", Collections.singletonMap("not_date", fieldCaps));
        RollupActionGroupConfig config = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval("date_field", DateHistogramInterval.DAY)
        );
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(1));
    }

    public void testValidatesAllSubConfigs() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("date_field", Collections.singletonMap("date", fieldCaps));
        responseMap.put("terms_field", Collections.singletonMap("keyword", fieldCaps));
        responseMap.put("histogram_field", Collections.singletonMap("keyword", fieldCaps));
        RollupActionGroupConfig config = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval("date_field", DateHistogramInterval.DAY),
            new HistogramGroupConfig(132, "histogram_field"),
            new TermsGroupConfig("terms_field")
        );
        config.validateMappings(responseMap, e);
        // all fields are non-aggregatable
        assertThat(e.validationErrors().size(), equalTo(3));
        assertThat(e.validationErrors().get(0), equalTo("The field [date_field] must be aggregatable, but is not."));
        assertThat(
            e.validationErrors().get(1),
            equalTo("The field referenced by a histo group must be a [numeric] type, " + "but found [keyword] for field [histogram_field]")
        );
        assertThat(e.validationErrors().get(2), equalTo("The field [terms_field] must be aggregatable across all indices, but is not."));
    }
}

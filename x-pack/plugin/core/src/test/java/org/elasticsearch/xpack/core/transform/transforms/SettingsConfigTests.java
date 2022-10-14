/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SettingsConfigTests extends AbstractSerializingTransformTestCase<SettingsConfig> {

    private boolean lenient;

    public static SettingsConfig randomSettingsConfig() {
        Integer unattended = randomBoolean() ? null : randomIntBetween(0, 1);

        return new SettingsConfig(
            randomBoolean() ? null : randomIntBetween(10, 10_000),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : randomIntBetween(0, 1),
            randomBoolean() ? null : randomIntBetween(0, 1),
            randomBoolean() ? null : randomIntBetween(0, 1),
            randomBoolean() ? null : randomIntBetween(0, 1),
            // don't set retries if unattended is set to true
            randomBoolean() ? null : Integer.valueOf(1).equals(unattended) ? null : randomIntBetween(-1, 100),
            unattended
        );
    }

    public static SettingsConfig randomNonEmptySettingsConfig() {
        Integer unattended = randomIntBetween(0, 1);

        return new SettingsConfig(
            randomIntBetween(10, 10_000),
            randomFloat(),
            randomIntBetween(0, 1),
            randomIntBetween(0, 1),
            randomIntBetween(0, 1),
            randomIntBetween(0, 1),
            Integer.valueOf(1).equals(unattended) ? -1 : randomIntBetween(-1, 100),
            unattended
        );
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected SettingsConfig doParseInstance(XContentParser parser) throws IOException {
        return SettingsConfig.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected SettingsConfig createTestInstance() {
        return randomSettingsConfig();
    }

    @Override
    protected Reader<SettingsConfig> instanceReader() {
        return SettingsConfig::new;
    }

    public void testExplicitNullParsing() throws IOException {

        // explicit null
        assertThat(fromString("{\"max_page_search_size\" : null}").getMaxPageSearchSize(), equalTo(-1));
        // not set
        assertNull(fromString("{}").getMaxPageSearchSize());

        assertThat(fromString("{\"docs_per_second\" : null}").getDocsPerSecond(), equalTo(-1F));
        assertNull(fromString("{}").getDocsPerSecond());

        assertThat(fromString("{\"dates_as_epoch_millis\" : null}").getDatesAsEpochMillisForUpdate(), equalTo(-1));
        assertNull(fromString("{}").getDatesAsEpochMillisForUpdate());

        assertThat(fromString("{\"align_checkpoints\" : null}").getAlignCheckpointsForUpdate(), equalTo(-1));
        assertNull(fromString("{}").getAlignCheckpointsForUpdate());

        assertThat(fromString("{\"use_point_in_time\" : null}").getUsePitForUpdate(), equalTo(-1));
        assertNull(fromString("{}").getUsePitForUpdate());

        assertThat(fromString("{\"deduce_mappings\" : null}").getDeduceMappingsForUpdate(), equalTo(-1));
        assertNull(fromString("{}").getDeduceMappingsForUpdate());

        assertNull(fromString("{\"num_failure_retries\" : null}").getNumFailureRetries());
        assertThat(fromString("{\"num_failure_retries\" : null}").getNumFailureRetriesForUpdate(), equalTo(-2));
        assertNull(fromString("{}").getNumFailureRetries());
        assertNull(fromString("{}").getNumFailureRetriesForUpdate());
    }

    public void testUpdateMaxPageSearchSizeUsingBuilder() throws IOException {
        SettingsConfig config = fromString(
            "{\"max_page_search_size\": 10000, "
                + "\"docs_per_second\": 42, "
                + "\"dates_as_epoch_millis\": true, "
                + "\"align_checkpoints\": false,"
                + "\"use_point_in_time\": false,"
                + "\"deduce_mappings\": false,"
                + "\"num_failure_retries\": 5,"
                + "\"unattended\": false}"
        );
        SettingsConfig.Builder builder = new SettingsConfig.Builder(config);
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, 5, false))));

        builder.update(fromString("{\"max_page_search_size\": 100}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(100, 42F, true, false, false, false, 5, false))));
        assertThat(builder.build().getDatesAsEpochMillisForUpdate(), equalTo(1));
        assertThat(builder.build().getAlignCheckpointsForUpdate(), equalTo(0));
        assertThat(builder.build().getUsePitForUpdate(), equalTo(0));
        assertThat(builder.build().getDeduceMappingsForUpdate(), equalTo(0));
        assertThat(builder.build().getUnattendedForUpdate(), equalTo(0));

        builder.update(fromString("{\"max_page_search_size\": null}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(null, 42F, true, false, false, false, 5, false))));
        assertThat(builder.build().getDatesAsEpochMillisForUpdate(), equalTo(1));
        assertThat(builder.build().getAlignCheckpointsForUpdate(), equalTo(0));
        assertThat(builder.build().getUsePitForUpdate(), equalTo(0));
        assertThat(builder.build().getDeduceMappingsForUpdate(), equalTo(0));
        assertThat(builder.build().getUnattendedForUpdate(), equalTo(0));

        builder.update(
            fromString(
                "{\"max_page_search_size\": 77, "
                    + "\"docs_per_second\": null, "
                    + "\"dates_as_epoch_millis\": null, "
                    + "\"align_checkpoints\": null,"
                    + "\"use_point_in_time\": null,"
                    + "\"deduce_mappings\": null,"
                    + "\"num_failure_retries\": null,"
                    + "\"unattended\": null}"
            )
        );
        assertThat(builder.build(), is(equalTo(new SettingsConfig(77, null, (Boolean) null, null, null, null, null, null))));
        assertNull(builder.build().getDatesAsEpochMillisForUpdate());
        assertNull(builder.build().getAlignCheckpointsForUpdate());
        assertNull(builder.build().getUsePitForUpdate());
        assertNull(builder.build().getDeduceMappingsForUpdate());
        assertNull(builder.build().getNumFailureRetriesForUpdate());
        assertNull(builder.build().getUnattendedForUpdate());
    }

    public void testUpdateNumFailureRetriesUsingBuilder() throws IOException {
        SettingsConfig config = fromString(
            "{\"max_page_search_size\": 10000, "
                + "\"docs_per_second\": 42, "
                + "\"dates_as_epoch_millis\": true, "
                + "\"align_checkpoints\": false,"
                + "\"use_point_in_time\": false,"
                + "\"deduce_mappings\": false,"
                + "\"num_failure_retries\": 5}"
        );
        SettingsConfig.Builder builder = new SettingsConfig.Builder(config);
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, 5, null))));

        builder.update(fromString("{\"num_failure_retries\": 6}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, 6, null))));

        builder.update(fromString("{\"num_failure_retries\": -1}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, -1, null))));

        builder.update(fromString("{\"num_failure_retries\": null}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, null, null))));

        builder.update(fromString("{\"num_failure_retries\": 55}"));
        assertThat(builder.build(), is(equalTo(new SettingsConfig(10000, 42F, true, false, false, false, 55, null))));
    }

    public void testOmmitDefaultsOnWriteParser() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = fromString("{\"max_page_search_size\" : null}");
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        SettingsConfig emptyConfig = fromString("{}");
        assertNull(emptyConfig.getMaxPageSearchSize());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"docs_per_second\" : null}");
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"dates_as_epoch_millis\" : null}");
        assertThat(config.getDatesAsEpochMillisForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"align_checkpoints\" : null}");
        assertThat(config.getAlignCheckpointsForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"use_point_in_time\" : null}");
        assertThat(config.getUsePitForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"deduce_mappings\" : null}");
        assertThat(config.getDeduceMappingsForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"num_failure_retries\" : null}");
        assertThat(config.getNumFailureRetries(), nullValue());
        assertThat(config.getNumFailureRetriesForUpdate(), equalTo(-2));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"unattended\" : null}");
        assertThat(config.getUnattendedForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());
    }

    public void testOmmitDefaultsOnWriteBuilder() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = new SettingsConfig.Builder().setMaxPageSearchSize(null).build();
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        SettingsConfig emptyConfig = new SettingsConfig.Builder().build();
        assertNull(emptyConfig.getMaxPageSearchSize());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setRequestsPerSecond(null).build();
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setDatesAsEpochMillis(null).build();
        assertThat(config.getDatesAsEpochMillisForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setAlignCheckpoints(null).build();
        assertThat(config.getAlignCheckpointsForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setUsePit(null).build();
        assertThat(config.getUsePitForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setDeduceMappings(null).build();
        assertThat(config.getDeduceMappingsForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setNumFailureRetries(null).build();
        assertThat(config.getNumFailureRetries(), nullValue());
        assertThat(config.getNumFailureRetriesForUpdate(), equalTo(-2));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setUnattended(null).build();
        assertThat(config.getUnattendedForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());
    }

    public void testValidateMaxPageSearchSize() {
        SettingsConfig config = new SettingsConfig.Builder().build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setMaxPageSearchSize(null).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.max_page_search_size [-1] is out of range. The minimum value is 10 and the maximum is 65536")
        );

        config = new SettingsConfig.Builder().setMaxPageSearchSize(-2).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.max_page_search_size [-2] is out of range. The minimum value is 10 and the maximum is 65536")
        );

        config = new SettingsConfig.Builder().setMaxPageSearchSize(-1).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.max_page_search_size [-1] is out of range. The minimum value is 10 and the maximum is 65536")
        );

        config = new SettingsConfig.Builder().setMaxPageSearchSize(0).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.max_page_search_size [0] is out of range. The minimum value is 10 and the maximum is 65536")
        );

        config = new SettingsConfig.Builder().setMaxPageSearchSize(10).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setMaxPageSearchSize(65536).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setMaxPageSearchSize(65537).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.max_page_search_size [65537] is out of range. The minimum value is 10 and the maximum is 65536")
        );
    }

    public void testValidateNumFailureRetries() {
        SettingsConfig config = new SettingsConfig.Builder().build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setNumFailureRetries(null).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.num_failure_retries [-2] is out of range. The minimum value is -1 (infinity) and the maximum is 100")
        );

        config = new SettingsConfig.Builder().setNumFailureRetries(-2).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.num_failure_retries [-2] is out of range. The minimum value is -1 (infinity) and the maximum is 100")
        );

        config = new SettingsConfig.Builder().setNumFailureRetries(-1).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setNumFailureRetries(0).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setNumFailureRetries(1).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setNumFailureRetries(100).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setNumFailureRetries(101).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.num_failure_retries [101] is out of range. The minimum value is -1 (infinity) and the maximum is 100")
        );
    }

    public void testValidateUnattended() {
        SettingsConfig config = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(20).build();
        assertThat(
            config.validate(null).validationErrors(),
            contains("settings.num_failure_retries [20] can not be set in unattended mode, unattended retries indefinitely")
        );

        config = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(-1).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setUnattended(null).setNumFailureRetries(10).build();
        assertThat(config.validate(null), is(nullValue()));

        config = new SettingsConfig.Builder().setUnattended(false).setNumFailureRetries(10).build();
        assertThat(config.validate(null), is(nullValue()));
    }

    public void testUnattendedVariants() {
        SettingsConfig config = new SettingsConfig.Builder().build();
        assertNull(config.getUnattended());

        config = new SettingsConfig.Builder().setUnattended(null).build();
        assertNull(config.getUnattended());

        config = new SettingsConfig.Builder().setUnattended(true).build();
        assertTrue(config.getUnattended());

        config = new SettingsConfig.Builder().setUnattended(false).build();
        assertFalse(config.getUnattended());
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, XContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private SettingsConfig fromString(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            return SettingsConfig.fromXContent(parser, false);
        }
    }
}

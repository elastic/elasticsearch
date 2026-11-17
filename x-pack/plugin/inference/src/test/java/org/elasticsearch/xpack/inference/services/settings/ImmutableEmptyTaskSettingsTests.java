/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ImmutableEmptyTaskSettingsTests extends AbstractBWCSerializationTestCase<ImmutableEmptyTaskSettings> {

    private static final Map<String, Object> NON_EMPTY_SETTINGS = Map.of("unknown1", "value", "unknown2", "value2");
    private static final String UNKNOWN_FIELD_ERROR_MESSAGE = Strings.format(
        "[%s] Configuration contains unknown settings %s",
        ModelConfigurations.TASK_SETTINGS,
        NON_EMPTY_SETTINGS.keySet()
    );

    @Override
    protected Writeable.Reader<ImmutableEmptyTaskSettings> instanceReader() {
        return ImmutableEmptyTaskSettings::new;
    }

    @Override
    protected ImmutableEmptyTaskSettings createTestInstance() {
        return ImmutableEmptyTaskSettings.INSTANCE;
    }

    @Override
    protected ImmutableEmptyTaskSettings mutateInstance(ImmutableEmptyTaskSettings instance) {
        // All instances are the same and have no fields, nothing to mutate
        return null;
    }

    @Override
    protected ImmutableEmptyTaskSettings doParseInstance(XContentParser parser) throws IOException {
        // ImmutableEmptyTaskSettings has no fields; advance past START_OBJECT, drain it, return a fresh instance.
        // A new instance equals INSTANCE because the record has no components and its equals() compares by value.
        parser.nextToken();
        parser.map();
        return new ImmutableEmptyTaskSettings();
    }

    @Override
    protected ImmutableEmptyTaskSettings mutateInstanceForVersion(ImmutableEmptyTaskSettings instance, TransportVersion version) {
        // writeTo is a no-op, so the bytes are identical on every version
        return instance;
    }

    public void testIsEmpty() {
        assertTrue(ImmutableEmptyTaskSettings.INSTANCE.isEmpty());
    }

    public void testToXContent_ProducesEmptyObject() {
        assertThat(Strings.toString(ImmutableEmptyTaskSettings.INSTANCE), is("{}"));
    }

    public void testFromMap_EmptyMap_RequestContext_ReturnsInstance() {
        var settings = ImmutableEmptyTaskSettings.fromMap(Map.of(), ConfigurationParseContext.REQUEST);
        assertThat(settings, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_EmptyMap_PersistentContext_ReturnsInstance() {
        var settings = ImmutableEmptyTaskSettings.fromMap(Map.of(), ConfigurationParseContext.PERSISTENT);
        assertThat(settings, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_NonEmptyMap_PersistentContext_ReturnsInstance() {
        var settings = ImmutableEmptyTaskSettings.fromMap(NON_EMPTY_SETTINGS, ConfigurationParseContext.PERSISTENT);
        assertThat(settings, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_NonEmptyMap_RequestContext_Throws() {
        var ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> ImmutableEmptyTaskSettings.fromMap(NON_EMPTY_SETTINGS, ConfigurationParseContext.REQUEST)
        );
        assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), containsString(UNKNOWN_FIELD_ERROR_MESSAGE));
    }

    public void testUpdatedTaskSettings_EmptyMap_ReturnsInstance() {
        var updated = ImmutableEmptyTaskSettings.INSTANCE.updatedTaskSettings(Map.of());
        assertThat(updated, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    public void testUpdatedTaskSettings_NonEmptyMap_Throws() {
        var ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> ImmutableEmptyTaskSettings.INSTANCE.updatedTaskSettings(NON_EMPTY_SETTINGS)
        );
        assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), containsString(UNKNOWN_FIELD_ERROR_MESSAGE));
    }
}

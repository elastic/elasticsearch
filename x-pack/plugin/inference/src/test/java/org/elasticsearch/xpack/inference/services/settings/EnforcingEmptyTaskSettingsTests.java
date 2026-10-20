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
import org.elasticsearch.inference.EmptyTaskSettings;
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

public class EnforcingEmptyTaskSettingsTests extends AbstractBWCSerializationTestCase<EnforcingEmptyTaskSettings> {

    private static final Map<String, Object> NON_EMPTY_SETTINGS = Map.of("unknown1", "value", "unknown2", "value2");
    private static final String UNKNOWN_FIELD_ERROR_MESSAGE = Strings.format(
        "[%s] Configuration contains unknown settings %s",
        ModelConfigurations.TASK_SETTINGS,
        NON_EMPTY_SETTINGS.keySet()
    );

    @Override
    protected Writeable.Reader<EnforcingEmptyTaskSettings> instanceReader() {
        return EnforcingEmptyTaskSettings::new;
    }

    @Override
    protected EnforcingEmptyTaskSettings createTestInstance() {
        return EnforcingEmptyTaskSettings.INSTANCE;
    }

    @Override
    protected EnforcingEmptyTaskSettings mutateInstance(EnforcingEmptyTaskSettings instance) {
        // All instances are the same and have no fields, nothing to mutate
        return null;
    }

    @Override
    protected EnforcingEmptyTaskSettings doParseInstance(XContentParser parser) throws IOException {
        // EnforcingEmptyTaskSettings has no fields; advance past START_OBJECT, drain it, return a fresh instance.
        // A new instance equals INSTANCE because equals() compares by class, not by reference.
        parser.nextToken();
        parser.map();
        return new EnforcingEmptyTaskSettings();
    }

    @Override
    protected EnforcingEmptyTaskSettings mutateInstanceForVersion(EnforcingEmptyTaskSettings instance, TransportVersion version) {
        // writeTo is a no-op, so the bytes are identical on every version
        return instance;
    }

    public void testIsEmpty() {
        assertTrue(EnforcingEmptyTaskSettings.INSTANCE.isEmpty());
    }

    public void testToXContent_ProducesEmptyObject() {
        assertThat(Strings.toString(EnforcingEmptyTaskSettings.INSTANCE), is("{}"));
    }

    public void testFromMap_EmptyMap_RequestContext_ReturnsInstance() {
        var settings = EnforcingEmptyTaskSettings.fromMap(Map.of(), ConfigurationParseContext.REQUEST);
        assertThat(settings, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_EmptyMap_PersistentContext_ReturnsInstance() {
        var settings = EnforcingEmptyTaskSettings.fromMap(Map.of(), ConfigurationParseContext.PERSISTENT);
        assertThat(settings, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_NonEmptyMap_PersistentContext_ReturnsInstance() {
        var settings = EnforcingEmptyTaskSettings.fromMap(NON_EMPTY_SETTINGS, ConfigurationParseContext.PERSISTENT);
        assertThat(settings, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testFromMap_NonEmptyMap_RequestContext_Throws() {
        var ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> EnforcingEmptyTaskSettings.fromMap(NON_EMPTY_SETTINGS, ConfigurationParseContext.REQUEST)
        );
        assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), containsString(UNKNOWN_FIELD_ERROR_MESSAGE));
    }

    public void testUpdatedTaskSettings_EmptyMap_ReturnsInstance() {
        var updated = EnforcingEmptyTaskSettings.INSTANCE.updatedTaskSettings(Map.of());
        assertThat(updated, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testUpdatedTaskSettings_NonEmptyMap_Throws() {
        var ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> EnforcingEmptyTaskSettings.INSTANCE.updatedTaskSettings(NON_EMPTY_SETTINGS)
        );
        assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), containsString(UNKNOWN_FIELD_ERROR_MESSAGE));
    }

    public void testGetWriteableName_MatchesEmptyTaskSettings() {
        // Deliberate: EnforcingEmptyTaskSettings reuses EmptyTaskSettings's writeable name/serialization so old
        // nodes can still read it back (as a plain EmptyTaskSettings) rather than failing on an unknown writeable.
        assertThat(EnforcingEmptyTaskSettings.INSTANCE.getWriteableName(), is(EmptyTaskSettings.NAME));
    }

    public void testNotEqualToEmptyTaskSettings() {
        // The two types must never compare equal: one silently accepts updates, the other rejects them.
        assertNotEquals(EnforcingEmptyTaskSettings.INSTANCE, EmptyTaskSettings.INSTANCE);
        assertNotEquals(EmptyTaskSettings.INSTANCE, EnforcingEmptyTaskSettings.INSTANCE);
    }
}

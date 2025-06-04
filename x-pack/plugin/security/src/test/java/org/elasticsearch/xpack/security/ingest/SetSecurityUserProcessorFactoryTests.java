/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor.Property;
import org.junit.Before;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SetSecurityUserProcessorFactoryTests extends ESTestCase {

    private SecurityContext securityContext;

    @Before
    public void setupContext() {
        securityContext = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));
    }

    public void testProcessor() throws Exception {
        SetSecurityUserProcessor.Factory factory = new SetSecurityUserProcessor.Factory(() -> securityContext, Settings.EMPTY);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        SetSecurityUserProcessor processor = factory.create(null, "_tag", null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(EnumSet.allOf(Property.class)));
    }

    public void testProcessor_noField() throws Exception {
        SetSecurityUserProcessor.Factory factory = new SetSecurityUserProcessor.Factory(() -> securityContext, Settings.EMPTY);
        Map<String, Object> config = new HashMap<>();
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, "_tag", null, config, null)
        );
        assertThat(e.getMetadata("es.property_name").get(0), equalTo("field"));
        assertThat(e.getMetadata("es.processor_type").get(0), equalTo(SetSecurityUserProcessor.TYPE));
        assertThat(e.getMetadata("es.processor_tag").get(0), equalTo("_tag"));
    }

    public void testProcessor_validProperties() throws Exception {
        SetSecurityUserProcessor.Factory factory = new SetSecurityUserProcessor.Factory(() -> securityContext, Settings.EMPTY);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", Arrays.asList(Property.USERNAME.name(), Property.ROLES.name()));
        SetSecurityUserProcessor processor = factory.create(null, "_tag", null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(EnumSet.of(Property.USERNAME, Property.ROLES)));
    }

    public void testProcessor_invalidProperties() throws Exception {
        SetSecurityUserProcessor.Factory factory = new SetSecurityUserProcessor.Factory(() -> securityContext, Settings.EMPTY);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", Arrays.asList("invalid"));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, "_tag", null, config, null)
        );
        assertThat(e.getMetadata("es.property_name").get(0), equalTo("properties"));
        assertThat(e.getMetadata("es.processor_type").get(0), equalTo(SetSecurityUserProcessor.TYPE));
        assertThat(e.getMetadata("es.processor_tag").get(0), equalTo("_tag"));
    }

    public void testCanConstructorProcessorWithoutSecurityEnabled() throws Exception {
        Settings securityDisabled = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        SetSecurityUserProcessor.Factory factory = new SetSecurityUserProcessor.Factory(() -> null, securityDisabled);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        final SetSecurityUserProcessor processor = factory.create(null, "_tag", null, config, null);
        assertThat(processor, notNullValue());
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.Autoscaling;

import java.io.IOException;

public class ReactiveStorageDeciderConfigurationSerializationTests extends AbstractSerializingTestCase<
    ReactiveStorageDeciderConfiguration> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new Autoscaling(Settings.EMPTY).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new Autoscaling(Settings.EMPTY).getNamedXContent());
    }

    @Override
    protected ReactiveStorageDeciderConfiguration doParseInstance(XContentParser parser) throws IOException {
        return ReactiveStorageDeciderConfiguration.parse(parser);
    }

    @Override
    protected Writeable.Reader<ReactiveStorageDeciderConfiguration> instanceReader() {
        return ReactiveStorageDeciderConfiguration::new;
    }

    @Override
    protected ReactiveStorageDeciderConfiguration createTestInstance() {
        return new ReactiveStorageDeciderConfiguration();
    }

    @Override
    protected ReactiveStorageDeciderConfiguration mutateInstance(ReactiveStorageDeciderConfiguration instance) throws IOException {
        return null;
    }
}

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

public class ReactiveStorageDeciderSerializationTests extends AbstractSerializingTestCase<ReactiveStorageDecider> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new Autoscaling(Settings.EMPTY).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new Autoscaling(Settings.EMPTY).getNamedXContent());
    }

    @Override
    protected ReactiveStorageDecider doParseInstance(XContentParser parser) throws IOException {
        return ReactiveStorageDecider.parse(parser);
    }

    @Override
    protected Writeable.Reader<ReactiveStorageDecider> instanceReader() {
        return ReactiveStorageDecider::new;
    }

    @Override
    protected ReactiveStorageDecider createTestInstance() {
        return new ReactiveStorageDecider(randomAlphaOfLength(8), randomAlphaOfLength(8));
    }

    @Override
    protected ReactiveStorageDecider mutateInstance(ReactiveStorageDecider instance) throws IOException {
        boolean mutateAttribute = randomBoolean();
        return new ReactiveStorageDecider(
            mutateAttribute ? randomValueOtherThan(instance.getTierAttribute(), () -> randomAlphaOfLength(8)) : instance.getTierAttribute(),
            mutateAttribute == false || randomBoolean()
                ? randomValueOtherThan(instance.getTier(), () -> randomAlphaOfLength(8))
                : instance.getTier()
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;

public class MatchConfigSerializationTests extends AbstractWireSerializingTestCase<MatchConfig> {

    private Configuration config;

    @Before
    public void initConfig() {
        this.config = randomConfiguration();
    }

    @Override
    protected Writeable.Reader<MatchConfig> instanceReader() {
        return MatchConfig::new;
    }

    @Override
    protected MatchConfig createTestInstance() {
        return randomMatchConfig();
    }

    private MatchConfig randomMatchConfig() {
        // Implement logic to create a random MatchConfig instance
        String name = randomAlphaOfLengthBetween(1, 100);
        int channel = randomInt();
        DataType type = randomValueOtherThanMany(t -> false == t.supportedVersion().supportedLocally(), () -> randomFrom(DataType.types()));
        return new MatchConfig(name, channel, type);
    }

    @Override
    protected MatchConfig mutateInstance(MatchConfig instance) {
        return mutateMatchConfig(instance);
    }

    private MatchConfig mutateMatchConfig(MatchConfig instance) {
        int i = randomIntBetween(1, 3);
        return switch (i) {
            case 1 -> {
                String name = randomValueOtherThan(instance.fieldName(), () -> randomAlphaOfLengthBetween(1, 100));
                yield new MatchConfig(name, instance.channel(), instance.type());
            }
            case 2 -> {
                int channel = randomValueOtherThan(instance.channel(), () -> randomInt());
                yield new MatchConfig(instance.fieldName(), channel, instance.type());
            }
            default -> {
                DataType type = randomValueOtherThan(instance.type(), () -> randomFrom(DataType.types()));
                yield new MatchConfig(instance.fieldName(), instance.channel(), type);
            }
        };
    }

    @Override
    protected MatchConfig copyInstance(MatchConfig instance, TransportVersion version) throws IOException {
        return copyInstance(instance, getNamedWriteableRegistry(), (out, v) -> v.writeTo(new PlanStreamOutput(out, config)), in -> {
            PlanStreamInput pin = new PlanStreamInput(in, in.namedWriteableRegistry(), config);
            return new MatchConfig(pin);
        }, version);
    }
}

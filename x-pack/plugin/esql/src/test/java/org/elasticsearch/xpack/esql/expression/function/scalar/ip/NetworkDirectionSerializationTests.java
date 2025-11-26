/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class NetworkDirectionSerializationTests extends AbstractExpressionSerializationTests<NetworkDirection> {

    @Override
    protected NetworkDirection createTestInstance() {
        Source source = randomSource();
        Expression sourceIpField = randomChild();
        Expression destinationIpField = randomChild();
        Expression internalNetworks = randomChild();
        return new NetworkDirection(source, sourceIpField, destinationIpField, internalNetworks);
    }

    @Override
    protected NetworkDirection mutateInstance(NetworkDirection instance) throws IOException {
        Source source = instance.source();
        Expression sourceIpField = instance.sourceIpField();
        Expression destinationIpField = instance.destinationIpField();
        Expression internalNetworks = instance.internalNetworks();
        switch (between(0, 2)) {
            case 0 -> sourceIpField = randomValueOtherThan(sourceIpField, AbstractExpressionSerializationTests::randomChild);
            case 1 -> destinationIpField = randomValueOtherThan(destinationIpField, AbstractExpressionSerializationTests::randomChild);
            case 2 -> internalNetworks = randomValueOtherThan(internalNetworks, AbstractExpressionSerializationTests::randomChild);
        }

        return new NetworkDirection(source, sourceIpField, destinationIpField, internalNetworks);
    }
}

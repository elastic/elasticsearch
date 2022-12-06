/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

@Experimental
public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    @FunctionalInterface
    interface Provider extends Describable {
        AggregatorFunction create(int inputChannel);

        @Override
        default String describe() {
            var description = getClass().getName();
            // FooBarAggregator --> fooBar
            description = description.substring(0, description.length() - 10);
            var startChar = Character.toLowerCase(description.charAt(0));
            if (startChar != description.charAt(0)) {
                description = startChar + description.substring(1);
            }
            return description;
        }
    }
}

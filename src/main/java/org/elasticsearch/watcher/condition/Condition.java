/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;

/**
 *
 */
public interface Condition extends ToXContent {

    String type();

    abstract class Result implements ToXContent {

        private final String type;
        protected final boolean met;

        public Result(String type, boolean met) {
            this.type = type;
            this.met = met;
        }

        public String type() {
            return type;
        }

        public boolean met() { return met; }

    }

    interface Builder<C extends Condition> {

        C build();
    }

    interface Field {
        ParseField MET = new ParseField("met");
    }
}

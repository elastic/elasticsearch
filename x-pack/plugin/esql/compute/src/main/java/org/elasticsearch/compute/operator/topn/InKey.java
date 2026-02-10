/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

enum InKey {
    NotInKey {
        @Override
        TopNEncoder mapEncoder(TopNEncoder base) {
            return base.toUnsortable();
        }

        @Override
        boolean inKey() {
            return false;
        }
    },
    InKeyAscending {
        @Override
        TopNEncoder mapEncoder(TopNEncoder base) {
            return base.toSortable(true);
        }

        @Override
        boolean inKey() {
            return true;
        }
    },
    InKeyDescending {
        @Override
        TopNEncoder mapEncoder(TopNEncoder base) {
            return base.toSortable(false);
        }

        @Override
        boolean inKey() {
            return true;
        }
    };

    abstract TopNEncoder mapEncoder(TopNEncoder base);

    abstract boolean inKey();
}

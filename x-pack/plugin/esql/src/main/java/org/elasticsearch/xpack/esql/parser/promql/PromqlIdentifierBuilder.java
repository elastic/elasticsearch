/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.IdentifierContext;

abstract class PromqlIdentifierBuilder extends AbstractPromqlBuilder {

    PromqlIdentifierBuilder(int startLine, int startColumn) {
        super(startLine, startColumn);
    }

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : ctx.getText();
    }
}

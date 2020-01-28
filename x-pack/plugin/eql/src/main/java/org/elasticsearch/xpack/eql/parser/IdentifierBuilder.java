/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IdentifierContext;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.getText());
    }

    private static String unquoteIdentifier(String identifier) {
        return identifier.replace("\"\"", "\"");
    }
}
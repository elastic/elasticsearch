/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.QualifiedNameContext;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.getText());
    }

    private static String unquoteIdentifier(String identifier) {
        return identifier.replace("\"\"", "\"");
    }

    @Override
    public String visitQualifiedName(QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }

        return Strings.collectionToDelimitedString(visitList(ctx.identifier(), String.class), ".");
    }
}
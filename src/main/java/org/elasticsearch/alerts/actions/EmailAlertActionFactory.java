/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EmailAlertActionFactory implements AlertActionFactory {

    @Override
    public AlertAction createAction(XContentParser parser) throws IOException {
        String display = null;
        List<String> addresses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "display":
                        display = parser.text();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (currentFieldName) {
                    case "addresses":
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            addresses.add(parser.text());
                        }
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
            }
        }
        return new EmailAlertAction(display, addresses.toArray(new String[addresses.size()]));
    }
}

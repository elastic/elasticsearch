/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

public interface GetBuiltinPrivilegesResponseTranslator {

    GetBuiltinPrivilegesResponse translate(GetBuiltinPrivilegesResponse response, boolean restrictResponse);

    class Default implements GetBuiltinPrivilegesResponseTranslator {
        public GetBuiltinPrivilegesResponse translate(GetBuiltinPrivilegesResponse response, boolean restrictResponse) {
            assert false == restrictResponse;
            return response;
        }
    }
}

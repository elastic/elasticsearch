/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.license;

public class GetLicenseResponse {

    private String license;

    GetLicenseResponse() {
    }

    public GetLicenseResponse(String license) {
        this.license = license;
    }

    public String getLicenseDefinition() {
        return license;
    }

}

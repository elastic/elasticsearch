/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.type;

//TODO: tuple, list of sequence, etc?
public enum DataType {

    SCALAR("SCALAR");
    
    private String resultType;
    
    DataType(String resultType) {
        this.resultType = resultType;
    }

    public String resultType() {
        return resultType;
    }
}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

class CaseSensitiveComparison extends FileAccessTreeComparison {

    CaseSensitiveComparison(char separatorChar) {
        super(Character::compare, separatorChar);
    }

    @Override
    protected boolean pathStartsWith(String pathString, String pathPrefix) {
        return pathString.startsWith(pathPrefix);
    }

    @Override
    protected boolean pathsAreEqual(String path1, String path2) {
        return path1.equals(path2);
    }
}

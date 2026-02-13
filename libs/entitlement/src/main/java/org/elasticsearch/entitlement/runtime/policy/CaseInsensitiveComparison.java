/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

class CaseInsensitiveComparison extends FileAccessTreeComparison {

    CaseInsensitiveComparison(char separatorChar) {
        super(CaseInsensitiveComparison::caseInsensitiveCharacterComparator, separatorChar);
    }

    private static int caseInsensitiveCharacterComparator(char c1, char c2) {
        return Character.compare(Character.toLowerCase(c1), Character.toLowerCase(c2));
    }

    @Override
    protected boolean pathStartsWith(String pathString, String pathPrefix) {
        return pathString.regionMatches(true, 0, pathPrefix, 0, pathPrefix.length());
    }

    @Override
    protected boolean pathsAreEqual(String path1, String path2) {
        return path1.equalsIgnoreCase(path2);
    }
}

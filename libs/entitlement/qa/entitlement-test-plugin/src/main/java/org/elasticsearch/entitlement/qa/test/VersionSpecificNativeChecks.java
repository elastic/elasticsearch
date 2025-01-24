/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

class VersionSpecificNativeChecks {

    static void enableNativeAccess() throws Exception {}

    static void addressLayoutWithTargetLayout() {}

    static void linkerDowncallHandle() {}

    static void linkerDowncallHandleWithAddress() {}

    static void linkerUpcallStub() throws NoSuchMethodException {}

    static void memorySegmentReinterpret() {}

    static void memorySegmentReinterpretWithCleanup() {}

    static void memorySegmentReinterpretWithSizeAndCleanup() {}

    static void symbolLookupWithPath() {}

    static void symbolLookupWithName() {}
}

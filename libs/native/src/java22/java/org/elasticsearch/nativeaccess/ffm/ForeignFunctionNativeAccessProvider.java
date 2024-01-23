/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffm;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.NativeAccessProvider;

public class ForeignFunctionNativeAccessProvider extends NativeAccessProvider {
    @Override
    protected NativeAccess loadLinuxNativeAccess() {
        return new ForeignFunctionLinuxNativeAccess();
    }

    @Override
    protected NativeAccess loadMacOSNativeAccess() {
        return new ForeignFunctionMacOSNativeAccess();
    }

    @Override
    protected NativeAccess loadWindowsNativeAccess() {
        return new ForeignFunctionWindowsNativeAccess();
    }
}

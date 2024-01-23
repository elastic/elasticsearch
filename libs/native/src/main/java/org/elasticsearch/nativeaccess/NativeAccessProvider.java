/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.core.internal.provider.ProviderLocator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Set;

public abstract class NativeAccessProvider {

    protected abstract NativeAccess loadLinuxNativeAccess();

    protected abstract NativeAccess loadMacOSNativeAccess();

    protected abstract NativeAccess loadWindowsNativeAccess();

    static NativeAccess getNativeAccess() {
        return Holder.INSTANCE;
    }

    static NativeAccess loadInstance() {
        var provider = loadProvider();
        var os = System.getProperty("os.name");
        if (os.startsWith("Linux")) {
            return provider.loadLinuxNativeAccess();
        } else if (os.startsWith("Mac OS")) {
            return provider.loadMacOSNativeAccess();
        } else if (os.startsWith("Windows")) {
            return provider.loadWindowsNativeAccess();
        }
        // unsupported os...what to do??
        return null;
    }

    static NativeAccessProvider loadProvider() {
        final int runtimeVersion = Runtime.version().feature();
        if (runtimeVersion >= 22) {
            return loadFFMImpl(runtimeVersion);
        }
        return loadJNAImpl();
    }

    private static NativeAccessProvider loadFFMImpl(int runtimeVersion) {
        try {
            var lookup = MethodHandles.lookup();
            var clazz = lookup.findClass("org.elasticsearch.nativeaccess.ffm.ForeignFunctionNativeAccessProvider");
            var constructor = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            try {
                return (NativeAccessProvider) constructor.invoke();
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new LinkageError("NativeAccessProvider for Java " + runtimeVersion + " has a bad constructor", e);
        } catch (ClassNotFoundException cnfe) {
            throw new LinkageError("NativeAccessProvider is missing for Java " + runtimeVersion, cnfe);
        }
    }

    private static NativeAccessProvider loadJNAImpl() {
        return new ProviderLocator<>(
            "native-access-jna",
            NativeAccessProvider.class,
            "org.elasticsearch.nativeaccess.jna",
            Set.of()).get();
    }

    private static final class Holder {
        private Holder() {}

        static final NativeAccess INSTANCE = loadInstance();
    }
}

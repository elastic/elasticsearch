/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

class LoadNativeLibrariesCheckActions {
    static void runtimeLoad() {
        try {
            Runtime.getRuntime().load("libSomeLibFile.so");
        } catch (UnsatisfiedLinkError ignored) {
            // The library does not exist, so we expect to fail loading it
        }
    }

    static void systemLoad() {
        try {
            System.load("libSomeLibFile.so");
        } catch (UnsatisfiedLinkError ignored) {
            // The library does not exist, so we expect to fail loading it
        }
    }

    static void runtimeLoadLibrary() {
        try {
            Runtime.getRuntime().loadLibrary("SomeLib");
        } catch (UnsatisfiedLinkError ignored) {
            // The library does not exist, so we expect to fail loading it
        }
    }

    static void systemLoadLibrary() {
        try {
            System.loadLibrary("SomeLib");
        } catch (UnsatisfiedLinkError ignored) {
            // The library does not exist, so we expect to fail loading it
        }
    }
}

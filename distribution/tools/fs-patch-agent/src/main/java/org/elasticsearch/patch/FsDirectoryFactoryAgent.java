/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.patch;

import java.lang.instrument.Instrumentation;

/**
 * Premain entry point for the FsDirectoryFactory patch agent.
 *
 * <p>Registers a {@link FsDirectoryFactoryTransformer} that substitutes the four pre-existing
 * {@code FsDirectoryFactory*} classes with their patched bytes as they are loaded, gated on a
 * SHA-256 fingerprint match against the known stock class files.
 *
 * <p>The patch also introduces a brand-new {@code FsDirectoryFactory$2} class. A
 * {@link java.lang.instrument.ClassFileTransformer} can only rewrite classes that are already
 * loadable, so it cannot introduce that class. Moreover {@code org.elasticsearch.index.store} is
 * owned by the named module {@code org.elasticsearch.server}, so a copy placed on the class path
 * (the unnamed module) would be invisible to it. That class is therefore injected into the module
 * out-of-band via {@code --patch-module} (a jvm.options.d entry in the cloud ESS image) rather than
 * by this agent.
 */
public final class FsDirectoryFactoryAgent {

    private FsDirectoryFactoryAgent() {}

    public static void premain(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new FsDirectoryFactoryTransformer());
        System.out.println("[fs-patch-agent] Attached; FsDirectoryFactory transformer registered");
    }
}

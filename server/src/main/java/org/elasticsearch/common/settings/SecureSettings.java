/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Set;

/**
 * An accessor for settings which are securely stored. See {@link SecureSetting}.
 */
public interface SecureSettings extends Closeable {

    /** Returns true iff the settings are loaded and retrievable. */
    boolean isLoaded();

    /** Returns the names of all secure settings available. */
    Set<String> getSettingNames();

    /** Return a string setting. The {@link SecureString} should be closed once it is used. */
    SecureString getString(String setting) throws GeneralSecurityException;

    /** Return a file setting. The {@link InputStream} should be closed once it is used. */
    InputStream getFile(String setting) throws GeneralSecurityException;

    byte[] getSHA256Digest(String setting) throws GeneralSecurityException;

    @Override
    void close() throws IOException;
}

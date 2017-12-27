/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Set;

/**
 * A container for settings which are securely stored at rest (on disk). See the
 * {@link SecureSetting} accessor.
 */
public interface SecureSettings extends AutoCloseable {

    /** Returns true iff the settings are retrievable. */
    boolean isUnlocked();

    /**
     * Renders settings retrievable. The password argument will be cleared. An
     * unlock call SHOULD be paired with a lock call, when settings are not to be
     * accessible anymore. The returned handle facilitates this; closing the
     * returned handle is equivalent to calling lock. Subsequent unlocks ARE
     * idempotent as are subsequent locks. Subsequent unlock-lock pairs MAY NOT be
     * supported by the implementation.
     */
    AutoCloseable unlock(char[] password) throws GeneralSecurityException, IOException;

    /**
     * Should be called as soon as the encapsulated settings are not needed further.
     * Subsequent calls are idempotent.
     */
    void lock();

    /**
     * Returns the names of all secure settings available. HAS to be available even
     * when the settings are not retrievable (aka locked). WILL NOT change during
     * the lifetime of the instance.
     */
    Set<String> getSettingNames();

    /**
     * Return a string setting. The {@link SecureString} should be closed once it is
     * used. It is a programming error to call this when the settings are locked.
     */
    SecureString getString(String setting) throws GeneralSecurityException;

    /**
     * Return a file setting. The {@link InputStream} should be closed once it is
     * used. It is a programming error to call this when the settings are locked.
     */
    InputStream getFile(String setting) throws GeneralSecurityException;

}

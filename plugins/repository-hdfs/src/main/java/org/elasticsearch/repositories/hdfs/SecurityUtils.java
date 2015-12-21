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

package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.FileContext;
import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

final class SecurityUtils {

    static <V> V execute(FileContextFactory fcf, FcCallback<V> callback) throws IOException {
        return execute(fcf.getFileContext(), callback);
    }

    static <V> V execute(FileContext fc, FcCallback<V> callback) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<V>() {
                @Override
                public V run() throws IOException {
                    return callback.doInHdfs(fc);
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getException();
        }
    }
}

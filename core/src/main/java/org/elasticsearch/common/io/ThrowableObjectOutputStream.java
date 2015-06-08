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

package org.elasticsearch.common.io;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

/**
 *
 */
public class ThrowableObjectOutputStream extends ObjectOutputStream {

    static final int TYPE_FAT_DESCRIPTOR = 0;
    static final int TYPE_THIN_DESCRIPTOR = 1;

    private static final String EXCEPTION_CLASSNAME = Exception.class.getName();
    static final int TYPE_EXCEPTION = 2;

    private static final String STACKTRACEELEMENT_CLASSNAME = StackTraceElement.class.getName();
    static final int TYPE_STACKTRACEELEMENT = 3;


    public ThrowableObjectOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    @Override
    protected void writeStreamHeader() throws IOException {
        writeByte(STREAM_VERSION);
    }

    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
        if (desc.getName().equals(EXCEPTION_CLASSNAME)) {
            write(TYPE_EXCEPTION);
        } else if (desc.getName().equals(STACKTRACEELEMENT_CLASSNAME)) {
            write(TYPE_STACKTRACEELEMENT);
        } else {
            Class<?> clazz = desc.forClass();
            if (clazz.isPrimitive() || clazz.isArray()) {
                write(TYPE_FAT_DESCRIPTOR);
                super.writeClassDescriptor(desc);
            } else {
                write(TYPE_THIN_DESCRIPTOR);
                writeUTF(desc.getName());
            }
        }
    }
}

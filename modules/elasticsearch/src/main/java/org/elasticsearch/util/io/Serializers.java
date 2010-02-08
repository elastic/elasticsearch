/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (Shay Banon)
 */
public final class Serializers {

    public static byte[] throwableToBytes(Throwable t) throws IOException {
        FastByteArrayOutputStream os = new FastByteArrayOutputStream();
        ThrowableObjectOutputStream oos = new ThrowableObjectOutputStream(os);
        oos.writeObject(t);
        oos.close();
        return os.unsafeByteArray();
    }

    public static Throwable throwableFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        FastByteArrayInputStream is = new FastByteArrayInputStream(bytes);
        ThrowableObjectInputStream ois = new ThrowableObjectInputStream(is);
        Throwable t = (Throwable) ois.readObject();
        ois.close();
        return t;
    }

    public static void objectToStream(Serializable obj, DataOutput out) throws IOException {
        byte[] bytes = objectToBytes(obj);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static Object objectFromStream(DataInput in) throws ClassNotFoundException, IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return objectFromBytes(bytes);
    }

    public static byte[] objectToBytes(Serializable obj) throws IOException {
        FastByteArrayOutputStream os = new FastByteArrayOutputStream();
        CompactObjectOutputStream oos = new CompactObjectOutputStream(os);
        oos.writeObject(obj);
        oos.close();
        return os.unsafeByteArray();
    }

    public static Object objectFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        FastByteArrayInputStream is = new FastByteArrayInputStream(bytes);
        CompactObjectInputStream ois = new CompactObjectInputStream(is);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }

    private Serializers() {

    }
}

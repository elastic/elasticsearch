/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.elasticsearch.core.PathUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

class TestPluginClassLoader extends ClassLoader {
    TestPluginClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name.startsWith("org.elasticsearch.plugins.analysis.DemoFilterIteratorFactory")
            || name.startsWith("org.elasticsearch.plugins.lucene")) {
            return getClass(name);
        }

        return super.loadClass(name);
    }

    private Class<?> getClass(String name) {
        String file = name.replace(".", PathUtils.getDefaultFileSystem().getSeparator()) + ".class";
        byte[] byteArr;
        try {
            byteArr = loadClassData(file);
            Class<?> c = defineClass(name, byteArr, 0, byteArr.length);
            resolveClass(c);
            return c;
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] loadClassData(String name) throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(name);
        if (stream == null) {
            return new byte[0];
        }
        int size = stream.available();
        byte[] buff = new byte[size];
        DataInputStream in = new DataInputStream(stream);
        in.readFully(buff);
        in.close();
        return buff;
    }
}

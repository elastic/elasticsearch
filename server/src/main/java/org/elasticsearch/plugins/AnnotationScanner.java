/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.plugins.PluginBundle;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class AnnotationScanner {
    public AnnotationScanner(PluginBundle bundle, ClassLoader pluginClassLoader) {

        for ( URL url : bundle.allUrls) {
            try {
                // build jar file name, then loop through zipped entries
                String jarFileName = URLDecoder.decode(url.getFile(), "UTF-8");
                JarFile jf  = new JarFile(jarFileName);
                Enumeration<JarEntry> jarEntries = jf.entries();
                while(jarEntries.hasMoreElements()) {
                    JarEntry jarEntry = jarEntries.nextElement();
                    try{
                        ClassReader cr = new ClassReader(jf.getInputStream(jarEntry));
                        cr.accept(annotationScanner,0);
                        cr.accept(interfaceScanner,0);
                    } catch (Throwable t) {
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.DynamicType
import org.junit.rules.ExternalResource
import org.junit.rules.TemporaryFolder

class LocalRepositoryFixture extends ExternalResource {

    private TemporaryFolder temporaryFolder

    LocalRepositoryFixture() {
        this.temporaryFolder = new TemporaryFolder()
    }

    @Override
    protected void before() throws Throwable {
        super.before()
        temporaryFolder.before()
    }

    @Override
    protected void after() {
        super.after()
        temporaryFolder.after()
    }

    void generateJar(String group, String module, String version, String... clazzNames) {
        def baseGroupFolderPath = group.replace('.', '/')
        def targetFolder = new File(repoDir, "${baseGroupFolderPath}/$module/$version")
        targetFolder.mkdirs()

        def jarFile = new File(targetFolder, "${module}-${version}.jar")
        if (clazzNames.size() == 0) {
            jarFile.write("blubb")
        } else {
            clazzNames.each { clazzName ->
                DynamicType.Unloaded<?> dynamicType = new ByteBuddy().subclass(Object.class)
                        .name(clazzName)
                        .make()
                if (jarFile.exists()) {
                    dynamicType.inject(jarFile);
                } else {
                    dynamicType.toJar(jarFile);
                }
            }
        }

    }

    void configureBuild(File buildFile) {
        buildFile << """
        repositories {
          maven {
            name = "local-test"
            url = "${getRepoDir().toURI()}"
            metadataSources {
              artifact()
            }
          }
        }  
        """
    }

    File getRepoDir() {
        new File(temporaryFolder.root, 'local-repo')
    }
}
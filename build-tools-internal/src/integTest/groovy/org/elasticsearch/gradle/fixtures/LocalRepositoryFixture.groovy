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

class LocalRepositoryFixture {

    private File rootProjectDir

    LocalRepositoryFixture(File rootProjectDir){
        this.rootProjectDir = rootProjectDir
    }

    void generateJar(String group, String module, String version, String... clazzNames){
        def baseGroupFolderPath = group.replace('.', '/')
        def targetFolder = new File(repoDir, "${baseGroupFolderPath}/$module/$version")
        targetFolder.mkdirs()

        def jarFile = new File(targetFolder, "${module}-${version}.jar")
        clazzNames.each {clazzName ->
            DynamicType.Unloaded<?> dynamicType = new ByteBuddy().subclass(Object.class)
                    .name(clazzName)
                    .make()
            if(jarFile.exists()) {
                dynamicType.inject(jarFile);
            }else {
                dynamicType.toJar(jarFile);
            }
        }
    }

    void configureBuild() {
        def buildFile = new File(rootProjectDir, 'build.gradle')
        buildFile << """
        repositories {
          maven {
            name = "local-test"
            url = file("local-repo")
            metadataSources {
              artifact()
            }
          }
        }  
        """
    }

    File getRepoDir() {
        new File(rootProjectDir, 'local-repo')
    }
}

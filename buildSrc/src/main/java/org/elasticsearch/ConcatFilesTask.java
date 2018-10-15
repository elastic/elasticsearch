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

package org.elasticsearch.gradle

import java.io.File;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.gradle.api.DefaultTask
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * Concatenates a list of files into one and removes duplicate lines.
 */
public class ConcatFilesTask extends DefaultTask {

    /** List of files to concatenate */
    FileTree files = new FileTree();

    /** line to add at the top of the target file */
    String headerLine;

    //OutputFile
    File target = new File();

    public ConcatFilesTask() {
        String description = "Concat a list of files into one.";
    }

    //TaskAction
    public void concatFiles() {
        final StringBuilder output = new StringBuilder();

        if (headerLine != null) {
            output.append(headerLine).append('\n');
        }

        final StringBuilder sb = new StringBuilder();
        for (File f: files) {
        	Scanner scanner = new Scanner(f, "UTF-8" );
        	String text = scanner.useDelimiter("\\A").next();
        	scanner.close(); 
        	sb.append(text);    	
        }

        
   
        // Remove duplicate lines 
        String s = sb.toString();
        String[] tokens = s.split("\n");
        Set<String> alreadyPresent = new HashSet<String>();
        StringBuilder resultBuilder = new StringBuilder();
        
        boolean first = true;
        for(String token : tokens) {

            if(!alreadyPresent.contains(token)) {
                if(first) first = false;
                else resultBuilder.append("\n");

                if(!alreadyPresent.contains(token))
                    resultBuilder.append(token);
            }

            alreadyPresent.add(token);
        }
        String target = resultBuilder.toString();
        
      }
}

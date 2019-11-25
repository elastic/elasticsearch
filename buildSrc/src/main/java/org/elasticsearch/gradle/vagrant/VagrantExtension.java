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

package org.elasticsearch.gradle.vagrant;

import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;

import java.io.File;
import java.util.Map;

public class VagrantExtension {

    private final Property<String> box;
    private final MapProperty<String, Object> hostEnv;
    private final MapProperty<String, Object> vmEnv;
    private final RegularFileProperty vagrantfile;
    private boolean isWindowsVM;

    public VagrantExtension(Project project) {
        this.box = project.getObjects().property(String.class);
        this.hostEnv = project.getObjects().mapProperty(String.class, Object.class);
        this.vmEnv = project.getObjects().mapProperty(String.class, Object.class);
        this.vagrantfile = project.getObjects().fileProperty();
        this.vagrantfile.convention(project.getRootProject().getLayout().getProjectDirectory().file("Vagrantfile"));
        this.isWindowsVM = false;
    }

    @Input
    public String getBox() {
        return box.get();
    }

    public void setBox(String box) {
        // TODO: should verify this against the Vagrantfile, but would need to do so in afterEvaluate once vagrantfile is unmodifiable
        this.box.set(box);
    }

    @Input
    public Map<String, Object> getHostEnv() {
        return hostEnv.get();
    }

    public void hostEnv(String name, Object value) {
        hostEnv.put(name, value);
    }

    @Input
    public Map<String, Object> getVmEnv() {
        return vmEnv.get();
    }

    public void vmEnv(String name, Object value) {
        vmEnv.put(name, value);
    }

    @Input
    public boolean isWindowsVM() {
        return isWindowsVM;
    }

    public void setIsWindowsVM(boolean isWindowsVM) {
        this.isWindowsVM = isWindowsVM;
    }

    @Input
    public File getVagrantfile() {
        return this.vagrantfile.get().getAsFile();
    }

    public void setVagrantfile(File file) {
        vagrantfile.set(file);
    }
}

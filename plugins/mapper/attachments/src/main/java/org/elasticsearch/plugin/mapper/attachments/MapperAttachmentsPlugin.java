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

package org.elasticsearch.plugin.mapper.attachments;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.util.guice.inject.Module;

import java.util.Collection;

import static org.elasticsearch.util.gcommon.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class MapperAttachmentsPlugin extends AbstractPlugin {

    @Override public String name() {
        return "mapper-attachments";
    }

    @Override public String description() {
        return "Adds the attachment type allowing to parse difference attachment formats";
    }

    @Override public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(MapperAttachmentsIndexModule.class);
        return modules;
    }
}

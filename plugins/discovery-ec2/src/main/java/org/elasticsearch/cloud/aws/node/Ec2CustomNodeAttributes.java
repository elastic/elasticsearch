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

package org.elasticsearch.cloud.aws.node;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cloud.aws.AwsEc2Service;
import org.elasticsearch.cloud.aws.AwsEc2ServiceImpl;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class Ec2CustomNodeAttributes extends AbstractComponent implements DiscoveryNodeService.CustomAttributesProvider {

    public Ec2CustomNodeAttributes(Settings settings) {
        super(settings);
    }

    @Override
    public Map<String, String> buildAttributes() {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings) == false) {
            return null;
        }
        Map<String, String> ec2Attributes = new HashMap<>();

        URLConnection urlConnection;
        InputStream in = null;
        try {
            URL url = new URL(AwsEc2ServiceImpl.EC2_METADATA_URL + "placement/availability-zone");
            logger.debug("obtaining ec2 [placement/availability-zone] from ec2 meta-data url {}", url);
            urlConnection = url.openConnection();
            urlConnection.setConnectTimeout(2000);
            in = urlConnection.getInputStream();
            BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

            String metadataResult = urlReader.readLine();
            if (metadataResult == null || metadataResult.length() == 0) {
                logger.error("no ec2 metadata returned from {}", url);
                return null;
            }
            ec2Attributes.put("aws_availability_zone", metadataResult);
        } catch (IOException e) {
            logger.debug("failed to get metadata for [placement/availability-zone]", e);
        } finally {
            IOUtils.closeWhileHandlingException(in);
        }

        return ec2Attributes;
    }
}

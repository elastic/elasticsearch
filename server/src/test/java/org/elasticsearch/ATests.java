/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;

public class ATests extends ESTestCase {

    public static class SizeOutputStream extends OutputStream {

        public long size;

        @Override
        public void write(int b) throws IOException {
            size++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            size += len;
        }
    }

    public void testA() throws IOException {
        byte[] bytes = ATests.class.getResourceAsStream("/a.json").readAllBytes();
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> builder = ImmutableOpenMap.builder();
        for (int i = 0; i < 20000; i++) {
            XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes);
            builder.put("a" + i, IndexTemplateMetadata.Builder.fromXContent(parser, "a" + i));
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().templates(builder.build())).build();
        state.toString();
        long start = System.currentTimeMillis();
        SizeOutputStream bos = new SizeOutputStream();
        XContentBuilder xb = new XContentBuilder(XContentType.SMILE.xContent(), bos);
        xb.startObject();
        state.toXContent(xb, ToXContent.EMPTY_PARAMS);
        xb.endObject();
        xb.close();
        long time = System.currentTimeMillis() - start;
        System.out.println("size: " + bos.size + " " + time);
    }
}

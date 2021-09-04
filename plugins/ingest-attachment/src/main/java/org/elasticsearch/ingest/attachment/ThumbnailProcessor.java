/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.ingest.attachment;

import co.elastic.thumbnails4j.core.Dimensions;
import co.elastic.thumbnails4j.core.Thumbnailer;
import co.elastic.thumbnails4j.doc.DOCThumbnailer;
//import co.elastic.thumbnails4j.docx.DOCXThumbnailer;
import co.elastic.thumbnails4j.image.ImageThumbnailer;
import co.elastic.thumbnails4j.pdf.PDFThumbnailer;
//import co.elastic.thumbnails4j.pptx.PPTXThumbnailer;
import co.elastic.thumbnails4j.xls.XLSThumbnailer;
import co.elastic.thumbnails4j.xlsx.XLSXThumbnailer;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.imageio.ImageIO;

import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class ThumbnailProcessor extends AbstractProcessor {
    public static final String TYPE = "thumbnail";

    private final String field;
    private final String targetField;
    private final Integer width;
    private final Integer height;
    private final String format;
    private final boolean ignoreMissing;

    ThumbnailProcessor(String tag, String description, String field, String targetField,
                       Integer width, Integer height, String format, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.width = width;
        this.height = height;
        this.format = format;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        byte[] input = ingestDocument.getFieldValueAsBytes(field, ignoreMissing);

        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse.");
        }

        ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            Thumbnailer thumbnailer;
            String mediaType = TikaImpl.detect(inputStream);
            switch (mediaType) {
                case "application/pdf":
                    thumbnailer = new PDFThumbnailer();
                    break;
                case "application/msword":
                    thumbnailer = new DOCThumbnailer();
                    break;
                case "application/excel":
                case "application/vnd.ms-excel":
                    thumbnailer = new XLSThumbnailer();
                    break;
                case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    thumbnailer = new XLSXThumbnailer();
                    break;
                // not working; xdocreport is causing issues with poi-ooxml-schemas somehow
                // case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                //    thumbnailer = new DOCXThumbnailer();
                //    break;
                // not working; java.security.AccessControlException: access denied ("java.lang.RuntimePermission" "getClassLoader")
                // case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                //    thumbnailer = new PPTXThumbnailer();
                //    break;
                case "image/gif":
                case "image/jpeg":
                case "image/png":
                    thumbnailer = new ImageThumbnailer(mediaType.substring(6));
                    break;
                default:
                    throw new ElasticsearchParseException("Unsupported media type [{}]", mediaType);
            }
            List<Dimensions> dimensions = Collections.singletonList(new Dimensions(width, height));
            BufferedImage thumbnail = thumbnailer.getThumbnails(inputStream, dimensions).get(0);
            ImageIO.write(thumbnail, format, outputStream);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", e, field);
        }

        String data = Base64.getEncoder().encodeToString(outputStream.toByteArray());

        ingestDocument.setFieldValue(targetField, data);

        return ingestDocument;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public ThumbnailProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                          String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "thumbnail");
            Integer width = readIntProperty(TYPE, processorTag, config, "width", 100);
            Integer height = readIntProperty(TYPE, processorTag, config, "height", 100);
            String format = readStringProperty(TYPE, processorTag, config, "format", "png");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            return new ThumbnailProcessor(processorTag, description, field, targetField, width, height, format, ignoreMissing);
        }
    }
}

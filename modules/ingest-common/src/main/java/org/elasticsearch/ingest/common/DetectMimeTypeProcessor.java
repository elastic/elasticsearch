/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MediaType;
import org.apache.tika.metadata.Metadata;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.ingest.Processor;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DetectMimeTypeProcessor extends AbstractProcessor {
    public static final String TYPE = "detect_mime_type";
    private static final MimeTypes MIME_TYPES = MimeTypes.getDefaultMimeTypes();
    private static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().configure(
        JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW,
        true
    ).build();
    private static final SAXParserFactory XML_FACTORY = SAXParserFactory.newInstance();
    static {
        XML_FACTORY.setValidating(false);
        XML_FACTORY.setNamespaceAware(true);
    }

    private final boolean ignoreMissing;
    private final boolean isBase64;
    private final String field;
    private final String targetField;

    DetectMimeTypeProcessor(String tag, String description, String field, String targetField, boolean isBase64, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.isBase64 = isBase64;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String mimeType = getMimeType(ingestDocument);
        if (mimeType == null) {
            if (ignoreMissing) {
                return ingestDocument;
            } else {
                throw new IllegalArgumentException("unable to get mime type for document");
            }
        }

        ingestDocument.setFieldValue(targetField, mimeType);
        return ingestDocument;
    }

    private String getMimeType(IngestDocument d) {
        String data = d.getFieldValue(field, String.class, ignoreMissing);
        if (data == null) {
            return null;
        }

        InputStream dataStream;
        if (isBase64) {
            dataStream = new ByteArrayInputStream(Base64.decodeBase64(data));
        } else {
            dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        }
        try {
            MediaType detected = MIME_TYPES.detect(dataStream, new Metadata());
            if (detected == null) {
                return null;
            }
            String mimeType = detected.toString();
            if (mimeType.equals("application/octet-stream")) {
                // this is a generic mime type for unrecognized byte streams
                // so just admit we detected nothing
                return null;
            }
            if (mimeType.equals("text/plain")) {
                // fallback and do some xml/json validation
                if (isValidJSON(dataStream)) {
                    return "application/json";
                }
                if (isValidXML(dataStream)) {
                    return "text/xml";
                }
            }
            if (mimeType.equals("application/x-msdownload")) {
                // we normalize this to application/vnd.microsoft.portable-executable
                // since that's the registered mime type
                // see: https://www.iana.org/assignments/media-types/application/vnd.microsoft.portable-executable
                return "application/vnd.microsoft.portable-executable";
            }
            return mimeType;
        } catch (IOException e) {
            return null;
        }
    }

    private boolean isValidJSON(InputStream dataStream) {
        try {
            dataStream.reset();
            final JsonParser parser = JSON_FACTORY.createParser(dataStream);
            while (parser.nextToken() != null) {}
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private boolean isValidXML(InputStream dataStream) {
        try {
            dataStream.reset();
            XML_FACTORY.newSAXParser().getXMLReader().parse(new InputSource(dataStream));
        } catch (IOException | SAXException | ParserConfigurationException e) {
            return false;
        }
        return true;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean getIsBase64() {
        return isBase64;
    }

    public boolean getIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public DetectMimeTypeProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            boolean isBase64 = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "base64_encoded", false);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            return new DetectMimeTypeProcessor(processorTag, description, field, targetField, isBase64, ignoreMissing);
        }
    }
}

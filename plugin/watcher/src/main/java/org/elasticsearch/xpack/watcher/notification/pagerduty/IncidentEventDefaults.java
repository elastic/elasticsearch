/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

/**
 * Get trigger default configurations either from global settings or specific account settings and merge them
 */
public class IncidentEventDefaults {

    final String description;
    final String incidentKey;
    final String client;
    final String clientUrl;
    final String eventType;
    final boolean attachPayload;
    final Context.LinkDefaults link;
    final Context.ImageDefaults image;

    public IncidentEventDefaults(Settings accountSettings) {
        description = accountSettings.get(IncidentEvent.Fields.DESCRIPTION.getPreferredName(), null);
        incidentKey = accountSettings.get(IncidentEvent.Fields.INCIDENT_KEY.getPreferredName(), null);
        client = accountSettings.get(IncidentEvent.Fields.CLIENT.getPreferredName(), null);
        clientUrl = accountSettings.get(IncidentEvent.Fields.CLIENT_URL.getPreferredName(), null);
        eventType = accountSettings.get(IncidentEvent.Fields.EVENT_TYPE.getPreferredName(), null);
        attachPayload = accountSettings.getAsBoolean(IncidentEvent.Fields.ATTACH_PAYLOAD.getPreferredName(), false);
        link = new Context.LinkDefaults(accountSettings.getAsSettings("link"));
        image = new Context.ImageDefaults(accountSettings.getAsSettings("image"));

    }

    static class Context {

        static class LinkDefaults {

            final String href;
            final String text;

            LinkDefaults(Settings settings) {
                href = settings.get(IncidentEventContext.XField.HREF.getPreferredName(), null);
                text = settings.get(IncidentEventContext.XField.TEXT.getPreferredName(), null);
            }

            @Override
            public int hashCode() {
                return Objects.hash(href, text);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()){
                    return false;
                }
                final LinkDefaults other = (LinkDefaults) obj;
                return Objects.equals(href, other.href) && Objects.equals(text, other.text);
            }
        }

        static class ImageDefaults {

            final String href;
            final String src;
            final String alt;

            ImageDefaults(Settings settings) {
                href = settings.get(IncidentEventContext.XField.HREF.getPreferredName(), null);
                src = settings.get(IncidentEventContext.XField.SRC.getPreferredName(), null);
                alt = settings.get(IncidentEventContext.XField.ALT.getPreferredName(), null);
            }

            @Override
            public int hashCode() {
                return Objects.hash(href, src, alt);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()){
                    return false;
                }
                final ImageDefaults other = (ImageDefaults) obj;
                return Objects.equals(href, other.href) && Objects.equals(src, other.src) && Objects.equals(alt, other.alt);
            }
        }
    }


}

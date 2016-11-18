/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.prelert.job.audit.AuditActivity;
import org.elasticsearch.xpack.prelert.job.audit.AuditMessage;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchAuditor implements Auditor
{
    private static final Logger LOGGER = Loggers.getLogger(ElasticsearchAuditor.class);

    private final Client client;
    private final String index;
    private final String jobId;

    public ElasticsearchAuditor(Client client, String index, String jobId)
    {
        this.client = Objects.requireNonNull(client);
        this.index = index;
        this.jobId = jobId;
    }

    @Override
    public void info(String message)
    {
        persistAuditMessage(AuditMessage.newInfo(jobId, message));
    }

    @Override
    public void warning(String message)
    {
        persistAuditMessage(AuditMessage.newWarning(jobId, message));
    }

    @Override
    public void error(String message)
    {
        persistAuditMessage(AuditMessage.newError(jobId, message));
    }

    @Override
    public void activity(String message)
    {
        persistAuditMessage(AuditMessage.newActivity(jobId, message));
    }

    @Override
    public void activity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors)
    {
        persistAuditActivity(AuditActivity.newActivity(totalJobs, totalDetectors, runningJobs, runningDetectors));
    }

    private void persistAuditMessage(AuditMessage message)
    {
        try
        {
            client.prepareIndex(index, AuditMessage.TYPE.getPreferredName())
            .setSource(serialiseMessage(message))
            .execute().actionGet();
        }
        catch (IOException | IndexNotFoundException e)
        {
            LOGGER.error("Error writing auditMessage", e);
        }
    }

    private void persistAuditActivity(AuditActivity activity)
    {
        try
        {
            client.prepareIndex(index, AuditActivity.TYPE.getPreferredName())
            .setSource(serialiseActivity(activity))
            .execute().actionGet();
        }
        catch (IOException | IndexNotFoundException e)
        {
            LOGGER.error("Error writing auditActivity", e);
        }
    }

    private XContentBuilder serialiseMessage(AuditMessage message) throws IOException
    {
        return message.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
    }

    private XContentBuilder serialiseActivity(AuditActivity activity) throws IOException
    {
        return activity.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
    }
}

package org.elastic.gradle

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory

import javax.inject.Inject

/**
 * A task to download files to s3, which allows delayed resolution of the s3 path
 */
class DownloadS3Task extends DefaultTask {


    private List<Object> toDownload = new ArrayList<>()

    @Input
    String bucket

    @Input
    File destDir

    /** True if the file paths should be flattened into a single directory when downloaded, false otherwise */
    @Input
    boolean flatten = false

    DownloadS3Task() {
        ext.set('needs.aws', true)
    }

    @Inject
    public ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException()
    }

    /**
     * Add a file to be uploaded to s3. The key object will be evaluated at runtime.
     *
     * If file is a directory, all files in the directory will be uploaded to the key as a prefix.
     */
    public void download(Object key) {
        toDownload.add(key)
    }

    @TaskAction
    public void downloadFromS3() {
        AWSCredentials creds = new BasicAWSCredentials(project.awsAccessKey, project.awsSecretKey)

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);

        AmazonS3Client client = new AmazonS3Client(creds, clientConfiguration);
        ProgressLogger progressLogger = getProgressLoggerFactory().newOperation("s3upload")
        progressLogger.description = "download files from s3"
        progressLogger.started()

        for (Object entry : toDownload) {
            String key = entry.toString()
            downloadFile(client, progressLogger, destDir, key)
        }
        progressLogger.completed()
    }

    /** Download a single file */
    private void downloadFile(AmazonS3Client client, ProgressLogger progressLogger, File destDir, String key) {
        String destPath
        if (flatten) {
            destPath = key.substring(key.lastIndexOf('/') + 1)
        } else {
            destPath = key
        }
        logger.info("Downloading ${destPath} from ${bucket}")
        progressLogger.progress("downloading ${destPath}")
        client.getObject(new GetObjectRequest(bucket, key),
                         new File(destDir, destPath))
    }
}

package org.elastic.gradle

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory

import javax.inject.Inject

/**
 * A task to upload files to s3, which allows delayed resolution of the s3 path
 */
class UploadS3Task extends DefaultTask {


    private Map<File, Object> toUpload = new LinkedHashMap<>()

    @Input
    String bucket

    /** True if a sha1 hash of each file should exist and be uploaded. This is ignored for uploading directories. */
    @Input
    boolean addSha1Hash = false

    /** True if a signature of each file should exist and be uploaded. This is ignored for uploading directories. */
    @Input
    boolean addSignature = false

    UploadS3Task() {
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
    public void upload(File file, Object key) {
        toUpload.put(file, key)
    }

    @TaskAction
    public void uploadToS3() {
        AWSCredentials creds = new BasicAWSCredentials(project.awsAccessKey, project.awsSecretKey)

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);

        AmazonS3Client client = new AmazonS3Client(creds, clientConfiguration);
        ProgressLogger progressLogger = getProgressLoggerFactory().newOperation("s3upload")
        progressLogger.description = "upload files to s3"
        progressLogger.started()

        for (Map.Entry<File, Object> entry : toUpload) {
            File file = entry.getKey()
            String key = entry.getValue().toString()
            if (file.isDirectory()) {
                uploadDir(client, progressLogger, file, key)
            } else {
                uploadFile(client, progressLogger, file, key)
                if (addSha1Hash) {
                    uploadFile(client, progressLogger, new File(file.path + '.sha1'), key + '.sha1')
                }
                if (addSignature) {
                    uploadFile(client, progressLogger, new File(file.path + '.asc'), key + '.asc')
                }
            }
        }
        progressLogger.completed()
    }

    /** Recursively upload all files in a directory. */
    private void uploadDir(AmazonS3Client client, ProgressLogger progressLogger, File dir, String prefix) {
        for (File subfile : dir.listFiles()) {
            if (subfile.isDirectory()) {
                uploadDir(client, progressLogger, subfile, "${prefix}/${subfile.name}")
            } else {
                String subkey = "${prefix}/${subfile.name}"
                uploadFile(client, progressLogger, subfile, subkey)
            }
        }
    }

    /** Upload a single file */
    private void uploadFile(AmazonS3Client client, ProgressLogger progressLogger, File file, String key) {
        logger.info("Uploading ${file.name} to ${key}")
        progressLogger.progress("uploading ${file.name}")
        client.putObject(bucket, key, file)
    }
}

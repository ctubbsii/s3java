package net.revelc.code.s3java;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class UploadBigFile {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: java UploadBigFile <existing-s3-bucket> <absolute-path-to-file>");
			System.exit(1);
		}
		String existingBucketName = args[0];
		String filePath = args[1];
		
		File uploadFile = new File(filePath);
		String keyName = null;
		if (uploadFile.exists() && uploadFile.canRead() && uploadFile.isFile()) {
			keyName = uploadFile.getName();
		}
		long totalSize = uploadFile.length();
		TransferManager tm = null;
		try {
			System.out.println("Creating transfer manager...");
			tm = TransferManagerBuilder.defaultTransferManager();
			
			// TransferManager processes all transfers asynchronously,
			// so this call will return immediately.
			Upload upload = tm.upload(existingBucketName, keyName, uploadFile);
			System.out.println("Starting upload...");

			// Or you can block and wait for the upload to finish
			upload.addProgressListener(new ProgressListener() {

				private long timeOfLastPrint = System.nanoTime();
				private AtomicLong bytesSinceLastPrint = new AtomicLong(0);
				private AtomicLong totalBytesTransferred = new AtomicLong(0);

				@Override
				public synchronized void progressChanged(ProgressEvent arg0) {
					long currentTime = System.nanoTime();
					double durationSinceLastPrint = (currentTime - timeOfLastPrint) / (1000.0 * 1000 * 1000);

					long diffAmt = arg0.getBytesTransferred();
					long amt = bytesSinceLastPrint.addAndGet(diffAmt);
					long totalTxfr = totalBytesTransferred.addAndGet(diffAmt);

					if (durationSinceLastPrint >= 10) {
						timeOfLastPrint = currentTime;
						System.out.printf("Transferred %d / %d bytes (%.1f%%). Speed: %.3fMBps\n", totalTxfr, totalSize,
								(100.0 * totalTxfr) / totalSize, amt / (1024 * 1024 * durationSinceLastPrint));
						bytesSinceLastPrint.set(0);
					}
				}
			});
			upload.waitForCompletion();
			System.out.println("Upload complete.");
		} catch (AmazonClientException amazonClientException) {
			System.out.println("Unable to upload file, upload was aborted.");
			amazonClientException.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("Shutting down...");
			if (tm != null) {
				tm.shutdownNow();
			}
			System.out.println("Done.");
		}
	}
}

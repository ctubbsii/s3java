package net.revelc.code.s3java;

import java.util.Date;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class AbortOldMultiParts {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println("Usage: java AbortOldMultiParts <existing-s3-bucket>");
			System.exit(1);
		}
		String existingBucketName = args[0];

		TransferManager tm = null;
		try {
			System.out.println("Creating transfer manager...");
			tm = TransferManagerBuilder.defaultTransferManager();
			
			System.out.println("Aborting old uploads...");
			tm.abortMultipartUploads(existingBucketName, new Date());

		} catch (AmazonClientException amazonClientException) {
			System.out.println("Unable to abort old uploads.");
			amazonClientException.printStackTrace();
		} finally {
			System.out.println("Shutting down...");
			if (tm != null) {
				tm.shutdownNow();
			}
			System.out.println("Done.");
		}
	}
}

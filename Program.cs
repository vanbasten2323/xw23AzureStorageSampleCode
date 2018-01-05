using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types
using Microsoft.Azure; //Namespace for CloudConfigurationManager
using System.IO;
using System.Linq;

namespace azureStorageAccount
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            // Parse the connection string and return a reference to the storage account.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Create container or get
            string containerName = "mycontainer";//"vhdblobs";

            //CloudBlobContainer container = CreateContainer(blobClient, "mycontainer");
            CloudBlobContainer container = GetContainerReference(blobClient, containerName);

            //PageBlobUploader.UploadVHDToCloud("Oracle-Linux-6-Generalized.128mb.variant.vhd", 128*1024*1024, blobClient, containerName, "Oracle-Linux-6-Generalized.128mb.variant.vhd");
            string blobName = "Oracle-Linux-6-Generalized.128mb.vhd";// "Oracle -Linux-6-Generalized.10GB.vhd";
            CloudPageBlob pageBlob = container.GetPageBlobReference(blobName);
            IEnumerable<PageRange> pageRanges = pageBlob.GetPageRanges();
            //PageBlobDownloader.DownloadVHDFromCloud(blobClient, containerName, blobName);
            /* 
        CloudPageBlob sourcePageBlob = container.GetPageBlobReference("blobwithrandomchar4mb.vhd");
        long size = GetCloudBlobSize(sourcePageBlob);
        byte[] blobData = new byte[size];
            sourcePageBlob.DownloadToByteArray(blobData, 0);

        CloudPageBlob destinationPageBlob = container.GetPageBlobReference("blobwithrandomchar4mb2.vhd");

        //bool isSame = VerifySourceBlobAndDestinationBlobIdentical(sourcePageBlob, destinationPageBlob);

        */
        bool isSame = VerifySourceBlobsAndDestinationBlobsIdentical(
            container, 
            new List<string>() { "Oracle-Linux-6-Generalized.128mb.vhd" },
            container, 
            new List<string>() { "Oracle-Linux-6-Generalized.128mb.vhd" });


            //PrintBlobSasUri(blobClient, containerName, "Oracle-Linux-6-Generalized.10GB.vhd");
            //PrintBlobSasUri(blobClient, "mycontainer", "blobwithrandomchar4mbvariant.vhd");


            Console.WriteLine("Reached the bottom.");
            Console.ReadLine();
        }

        public static CloudBlobContainer GetContainerReference(CloudBlobClient blobClient, string containerName)
        {
             return blobClient.GetContainerReference(containerName);
        }

        private static bool VerifySourceBlobsAndDestinationBlobsIdentical(CloudBlobContainer sourceBlobContainer,
            IList<string> sourceBlobNames,
            CloudBlobContainer destinationBlobContainer, IList<string> destinationBlobNames)
        {
            foreach (string sourceBlobName in sourceBlobNames)
            {
                CloudPageBlob sourceBlob = sourceBlobContainer.GetPageBlobReference(sourceBlobName);
                foreach (string destinationBlobName in destinationBlobNames)
                {
                    CloudPageBlob destinationBlob = destinationBlobContainer.GetPageBlobReference(destinationBlobName);
                    bool isSourceBlobAndDestinationBlobIdentical =
                        VerifySourceBlobAndDestinationBlobIdentical(sourceBlob, destinationBlob);
                    if (!isSourceBlobAndDestinationBlobIdentical)
                    {
                        return false;
                    }
                }
            }
            return true;
        }


        private static bool VerifySourceBlobAndDestinationBlobIdentical(CloudPageBlob sourceBlob, CloudPageBlob destinationBlob)
        {
            List<PageRange> sourcePageRanges = sourceBlob.GetPageRanges().ToList();
            List<PageRange> destinationPageRanges = destinationBlob.GetPageRanges().ToList();
            int numSourcePageRanges = sourcePageRanges.Count();
            int numDestinationPageRanges = destinationPageRanges.Count();
            if (numSourcePageRanges != numDestinationPageRanges)
            {
                return false;
            }
            sourcePageRanges = sourcePageRanges.OrderBy(pageRange => pageRange.StartOffset).ToList();
            destinationPageRanges = destinationPageRanges.OrderBy(pageRange => pageRange.StartOffset).ToList();
            for (int i = 0; i < numSourcePageRanges; ++i)
            {
                bool isPageRangeIdentical = ComparePageRange(sourceBlob, sourcePageRanges[i],
                    destinationBlob, destinationPageRanges[i]);
                if (!isPageRangeIdentical)
                {
                    return false;
                }
            }
            bool isBlobContentIdentical = CompareBlobContent(sourceBlob, destinationBlob);
            if (!isBlobContentIdentical)
            {
                return false;
            }
            return true;
        }

        private static bool ComparePageRange(CloudPageBlob pageBlob0, PageRange pageRange0,
            CloudPageBlob pageBlob1, PageRange pageRange1)
        {
            if (pageRange0.StartOffset != pageRange1.StartOffset ||
                pageRange0.EndOffset != pageRange1.EndOffset)
            {
                return false;
            }
            return true;
        }

        private static bool CompareBlobContent(CloudBlob blob0, CloudBlob blob1)
        {
            const int bytesToRead = sizeof(Int64);
            const int oneMegabyteInBytes = 1024*1024;
            long blobLength0 = GetCloudBlobSize(blob0);
            long blobLength1 = GetCloudBlobSize(blob1);
            if (blobLength0 != blobLength1)
            {
                return false;
            }

            // Need to read in chunks in order to avoid insufficient memory issue.
            long maxChunkSize = 64*oneMegabyteInBytes;
            int numChunks = (int) Math.Ceiling((double) blobLength0/maxChunkSize);
            byte[] bytes0 = new byte[bytesToRead];
            byte[] bytes1 = new byte[bytesToRead];
            long totalBytesRead = 0;
            while (totalBytesRead < blobLength0)
            {
                long bytesToBeRead = (blobLength0 - totalBytesRead < maxChunkSize)
                    ? (blobLength0 - totalBytesRead)
                    : maxChunkSize;
                using (Stream blobStream0 = new MemoryStream())
                using (Stream blobStream1 = new MemoryStream())
                {
                    blob0.DownloadRangeToStream(blobStream0, totalBytesRead, bytesToBeRead);
                    blob1.DownloadRangeToStream(blobStream1, totalBytesRead, bytesToBeRead);
                    blobStream0.Seek(0, SeekOrigin.Begin);
                    blobStream1.Seek(0, SeekOrigin.Begin);
                    int iterations = (int) Math.Ceiling((double) blobStream0.Length/bytesToRead);
                    for (int i = 0; i < iterations; i++)
                    {
                        blobStream0.Read(bytes0, 0, bytesToRead);
                        blobStream1.Read(bytes1, 0, bytesToRead);
                        if (BitConverter.ToInt64(bytes0, 0) != BitConverter.ToInt64(bytes1, 0))
                        {
                            return false;
                        }
                    }
                }
                totalBytesRead += bytesToBeRead;
            }
            return true;
        }

        /// <summary>
        /// THis is a working version for a more general blob not just page blob.
        /// </summary>
        /// <param name="sourceBlob"></param>
        /// <param name="destinationBlob"></param>
        /// <returns></returns>
        public static bool VerifySourceBlobAndDestinationBlobIdentical2(CloudBlob sourceBlob, CloudBlob destinationBlob)
        {
            const int bytesToRead = sizeof(Int64);

            const int numberOfPartition = 16;

            long sourceBlobLength = GetCloudBlobSize(sourceBlob);
            long destinationBlobLength = GetCloudBlobSize(destinationBlob);
            if (sourceBlobLength != destinationBlobLength)
            {
                return false;
            }
            int numberOfBytesInOnePartition = (int)Math.Ceiling((double)sourceBlobLength / numberOfPartition);
            int totalBytesRead = 0;
            while (totalBytesRead < sourceBlobLength)
            {
                using (Stream sourceBlobStream = new MemoryStream())
                using (Stream destinationBlobStream = new MemoryStream())
                {
                    //sourceBlob.DownloadToStream(sourceBlobStream, null, null);
                    //destinationBlob.DownloadToStream(destinationBlobStream, null, null);
                    sourceBlob.DownloadRangeToStream(sourceBlobStream, totalBytesRead, numberOfBytesInOnePartition);
                    destinationBlob.DownloadRangeToStream(destinationBlobStream, totalBytesRead, numberOfBytesInOnePartition);
                    sourceBlobStream.Seek(0, SeekOrigin.Begin);
                    destinationBlobStream.Seek(0, SeekOrigin.Begin);
                    long sourceBlobStreamLength = sourceBlobStream.Length;
                    long destinationBlobStreamLength = destinationBlobStream.Length;
                    if (sourceBlobStreamLength != destinationBlobStreamLength)
                    {
                        return false;
                    }

                    int iterations = (int)Math.Ceiling((double)sourceBlobStreamLength / bytesToRead);
                    byte[] sourceBytes = new byte[bytesToRead];
                    byte[] destinationBytes = new byte[bytesToRead];
                    for (int i = 0; i < iterations; i++)
                    {
                        sourceBlobStream.Read(sourceBytes, 0, bytesToRead);
                        destinationBlobStream.Read(destinationBytes, 0, bytesToRead);
                        if (BitConverter.ToInt64(sourceBytes, 0) != BitConverter.ToInt64(destinationBytes, 0))
                        {
                            return false;
                        }
                    }
                }
                totalBytesRead += numberOfBytesInOnePartition;
            }
            return true;
        }
        /*
        public static bool VerifySourceBlobAndDestinationBlobIdentical(CloudBlob sourceBlob, CloudBlob destinationBlob)
        {
            long sourceBlobLength = GetCloudBlobSize(sourceBlob);
            long destinationBlobLength = GetCloudBlobSize(destinationBlob);
            if (sourceBlobLength != destinationBlobLength)
            {
                return false;
            }
            int numberOfPartition = 16;
            int numberOfBytesInOnePartition = (int) Math.Ceiling((double)sourceBlobLength/numberOfPartition);
            int totalBytesRead = 0;
            while (totalBytesRead < sourceBlobLength)
            {
                using (Stream sourceBlobStream = new MemoryStream())
                using (Stream destinationBlobStream = new MemoryStream())
                {
                    //sourceBlob.DownloadToStream(sourceBlobStream, null, null);
                    //destinationBlob.DownloadToStream(destinationBlobStream, null, null);
                    sourceBlob.DownloadRangeToStream(sourceBlobStream, totalBytesRead, numberOfBytesInOnePartition);
                    destinationBlob.DownloadRangeToStream(destinationBlobStream, totalBytesRead, numberOfBytesInOnePartition);
                    sourceBlobStream.Seek(0, SeekOrigin.Begin);
                    destinationBlobStream.Seek(0, SeekOrigin.Begin);
                    long sourceBlobStreamLength = sourceBlobStream.Length;
                    long destinationBlobStreamLength = destinationBlobStream.Length;
                    if (sourceBlobStreamLength != destinationBlobStreamLength)
                    {
                        return false;
                    }

                    int iterations = (int) Math.Ceiling((double) sourceBlobStreamLength/BYTES_TO_READ);
                    byte[] sourceBytes = new byte[BYTES_TO_READ];
                    byte[] destinationBytes = new byte[BYTES_TO_READ];
                    for (int i = 0; i < iterations; i++)
                    {
                        sourceBlobStream.Read(sourceBytes, 0, BYTES_TO_READ);
                        destinationBlobStream.Read(destinationBytes, 0, BYTES_TO_READ);
                        if (BitConverter.ToInt64(sourceBytes, 0) != BitConverter.ToInt64(destinationBytes, 0))
                        {
                            return false;
                        }
                    }
                }
                totalBytesRead += numberOfBytesInOnePartition;
            }
            return true;
        }
        */

        /*
        public static bool CompareStreams(Stream stream0, Stream stream1)
        {
            
        }
        */

        public static long GetCloudBlobSize(CloudBlob blob)
        {
            if (blob.Exists())
            {
                blob.FetchAttributes();
                return blob.Properties.Length;
            }
            return -1;
        }

        private static FileInfo GetBlobFileInfo(CloudPageBlob blob)
        {
            return null;
        }

        const int BYTES_TO_READ = sizeof(Int64);

        /// <summary>
        /// This method compares if two file are identical.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="second"></param>
        /// <returns></returns>
        static bool FilesAreEqual(FileInfo first, FileInfo second)
        {
            if (first.Length != second.Length)
                return false;

            if (string.Equals(first.FullName, second.FullName, StringComparison.OrdinalIgnoreCase))
                return true;

            int iterations = (int)Math.Ceiling((double)first.Length / BYTES_TO_READ);

            using (FileStream fs1 = first.OpenRead())
            using (FileStream fs2 = second.OpenRead())
            {
                byte[] one = new byte[BYTES_TO_READ];
                byte[] two = new byte[BYTES_TO_READ];

                for (int i = 0; i < iterations; i++)
                {
                    fs1.Read(one, 0, BYTES_TO_READ);
                    fs2.Read(two, 0, BYTES_TO_READ);

                    if (BitConverter.ToInt64(one, 0) != BitConverter.ToInt64(two, 0))
                        return false;
                }
            }

            return true;
        }

        static void PrintBlobSasUri(CloudBlobClient blobClient, string containerName, string blobName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasUri = GetBlobSasUri(container, blobName, null);
            Console.WriteLine(sasUri);
        }

        static CloudBlobContainer CreateContainer(CloudBlobClient blobClient, string containerName)
        {
            // Retrieve a reference to a container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Create the container if it doesn't already exist.
            container.CreateIfNotExists();
            return container;
        }

        private static string GetBlobSasUri(CloudBlobContainer container, string blobName, string policyName = null)
        {
            string sasBlobToken;

            // Get a reference to a blob within the container.
            // Note that the blob may not exist yet, but a SAS can still be created for it.
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            if (policyName == null)
            {
                // Create a new access policy and define its constraints.
                // Note that the SharedAccessBlobPolicy class is used both to define the parameters of an ad-hoc SAS, and
                // to construct a shared access policy that is saved to the container's shared access policies.
                SharedAccessBlobPolicy adHocSAS = new SharedAccessBlobPolicy()
                {
                    // When the start time for the SAS is omitted, the start time is assumed to be the time when the storage service receives the request.
                    // Omitting the start time for a SAS that is effective immediately helps to avoid clock skew.
                    SharedAccessExpiryTime = DateTime.UtcNow.AddMonths(48),
                    Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Delete
                };

                // Generate the shared access signature on the blob, setting the constraints directly on the signature.
                sasBlobToken = blob.GetSharedAccessSignature(adHocSAS);

                Console.WriteLine("SAS for blob (ad hoc): {0}", sasBlobToken);
                Console.WriteLine();
            }
            else
            {
                // Generate the shared access signature on the blob. In this case, all of the constraints for the
                // shared access signature are specified on the container's stored access policy.
                sasBlobToken = blob.GetSharedAccessSignature(null, policyName);

                Console.WriteLine("SAS for blob (stored access policy): {0}", sasBlobToken);
                Console.WriteLine();
            }

            // Return the URI string for the container, including the SAS token.
            return blob.Uri + sasBlobToken;
        }
    }
}

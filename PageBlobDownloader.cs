using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace azureStorageAccount
{
    public static class PageBlobDownloader
    {
        public static void DownloadVHDFromCloud(CloudBlobClient blobStorage, string containerName, string blobName)
        {

            CloudBlobContainer container = blobStorage.GetContainerReference(containerName);
            CloudPageBlob pageBlob = container.GetPageBlobReference(blobName);

            // Get the length of the blob
            pageBlob.FetchAttributes();
            long vhdLength = pageBlob.Properties.Length;
            long totalDownloaded = 0;
            Console.WriteLine("Vhd size:  " + Megabytes(vhdLength));

            // Create a new local file to write into
            FileStream fileStream = new FileStream(blobName, FileMode.Create, FileAccess.Write);
            fileStream.SetLength(128 * OneMegabyteAsBytes);

            // Download the valid ranges of the blob, and write them to the file
            IEnumerable<PageRange> pageRanges = pageBlob.GetPageRanges();
            Stream blobStream = pageBlob.OpenRead();

            foreach (PageRange range in pageRanges)
            {
                // EndOffset is inclusive... so need to add 1
                int rangeSize = (int)(range.EndOffset + 1 - range.StartOffset);

                // Chop range into 4MB chucks, if needed
                for (int subOffset = 0; subOffset < rangeSize; subOffset += FourMegabyteAsBytes)
                {
                    int subRangeSize = Math.Min(rangeSize - subOffset, FourMegabyteAsBytes);
                    blobStream.Seek(range.StartOffset + subOffset, SeekOrigin.Begin);
                    fileStream.Seek(range.StartOffset + subOffset, SeekOrigin.Begin);

                    Console.WriteLine("Range: ~" + Megabytes(range.StartOffset + subOffset)
                                      + " + " + PrintSize(subRangeSize));
                    byte[] buffer = new byte[subRangeSize];

                    blobStream.Read(buffer, 0, subRangeSize);
                    fileStream.Write(buffer, 0, subRangeSize);
                    totalDownloaded += subRangeSize;
                    if (totalDownloaded > 128 * OneMegabyteAsBytes) break;
                }
                if (totalDownloaded > 128 * OneMegabyteAsBytes) break;
            }
            Console.WriteLine("Downloaded " + Megabytes(totalDownloaded) + " of " + Megabytes(vhdLength));
        }

        private static int OneMegabyteAsBytes = 1024 * 1024;
        private static int OneGigabyteAsBytes = 1024 * 1024 * 1024;
        private static int FourMegabyteAsBytes = 4 * OneMegabyteAsBytes;
        private static string Megabytes(long bytes)
        {
            return (bytes / OneMegabyteAsBytes).ToString() + " MB";
        }

        private static string PrintSize(long bytes)
        {
            if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).ToString() + " MB";
            if (bytes >= 1024) return (bytes / 1024).ToString() + " kb";
            return (bytes).ToString() + " bytes";
        }
    }
}

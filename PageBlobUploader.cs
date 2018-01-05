using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace azureStorageAccount
{
    public static class PageBlobUploader
    {
        private static bool IsAllZero(byte[] range, long rangeOffset, long size)
        {
            for (long offset = 0; offset < size; offset++)
            {
                if (range[rangeOffset + offset] != 0)
                {
                    return false;
                }
            }
            return true;
        }
        public static void UploadVHDToCloud(string fileName, long fileSize, 
            CloudBlobClient blobStorage, string containerName, string blobName)
        {
            CloudBlobContainer container = blobStorage.GetContainerReference(containerName);

            CloudPageBlob pageBlob = container.GetPageBlobReference(blobName);

            long blobSize = RoundUpToPageBlobSize(fileSize);
            pageBlob.Create(blobSize);

            FileStream stream = new FileStream(blobName, FileMode.Open, FileAccess.Read);
            BinaryReader reader = new BinaryReader(stream);

            long totalUploaded = 0;
            long vhdOffset = 0;
            int offsetToTransfer = -1;

            while (vhdOffset < fileSize)
            {
                byte[] range = reader.ReadBytes(FourMegabytesAsBytes);

                int offsetInRange = 0;

                // Make sure end is page size aligned
                if ((range.Length % PageBlobPageSize) > 0)
                {
                    int grow = (int)(PageBlobPageSize - (range.Length % PageBlobPageSize));
                    Array.Resize(ref range, range.Length + grow);
                }

                // Upload groups of contiguous non-zero page blob pages.  
                while (offsetInRange <= range.Length)
                {
                    if ((offsetInRange == range.Length) ||
                        IsAllZero(range, offsetInRange, PageBlobPageSize))
                    {
                        if (offsetToTransfer != -1)
                        {
                            // Transfer up to this point
                            int sizeToTransfer = offsetInRange - offsetToTransfer;
                            MemoryStream memoryStream = new MemoryStream(range,
                                         offsetToTransfer, sizeToTransfer, false, false);
                            pageBlob.WritePages(memoryStream, vhdOffset + offsetToTransfer);
                            Console.WriteLine("Range ~" + Megabytes(offsetToTransfer + vhdOffset)
                                    + " + " + PrintSize(sizeToTransfer));
                            totalUploaded += sizeToTransfer;
                            offsetToTransfer = -1;
                        }
                    }
                    else
                    {
                        if (offsetToTransfer == -1)
                        {
                            offsetToTransfer = offsetInRange;
                        }
                    }
                    offsetInRange += PageBlobPageSize;
                }
                vhdOffset += range.Length;
            }
            Console.WriteLine("Uploaded " + Megabytes(totalUploaded) + " of " + Megabytes(blobSize));
        }

        private static int PageBlobPageSize = 512;
        private static int OneMegabyteAsBytes = 1024 * 1024;
        private static int FourMegabytesAsBytes = 4 * OneMegabyteAsBytes;
        private static string PrintSize(long bytes)
        {
            if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).ToString() + " MB";
            if (bytes >= 1024) return (bytes / 1024).ToString() + " kb";
            return (bytes).ToString() + " bytes";
        }
        private static string Megabytes(long bytes)
        {
            return (bytes / OneMegabyteAsBytes).ToString() + " MB";
        }
        private static long RoundUpToPageBlobSize(long size)
        {
            return (size + PageBlobPageSize - 1) & ~(PageBlobPageSize - 1);
        }
    }
}

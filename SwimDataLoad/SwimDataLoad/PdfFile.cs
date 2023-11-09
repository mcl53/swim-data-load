using UglyToad.PdfPig;
using UglyToad.PdfPig.Content;

namespace SwimDataLoad
{
    class PdfFile
    {
        public readonly string FileName;
        public readonly string FileType;
        public List<PDFPage> Pages;

        public PdfFile(string pdfDir, string fileType)
        {
            FileName = pdfDir.Split("\\").Last();
            FileType = fileType;
            Pages = new List<PDFPage>();

            using PdfDocument pdfDoc = PdfDocument.Open(pdfDir);

            foreach (Page pdfPage in pdfDoc.GetPages())
            {
                Pages.Add(new PDFPage(pdfPage.Number, pdfPage.GetWords()));
            }
            FileType = fileType;
        }
    }

    internal readonly struct PDFPage
    {
        internal int PageNum { get; init; }
        internal IEnumerable<Word> Words { get; init; }

        internal PDFPage(int pageNum, IEnumerable<Word> words)
        {
            PageNum = pageNum;
            Words = words;
        }

        public override string ToString() => $"Page Number {PageNum}:\n{string.Join("\n", Words)}";
    }
}

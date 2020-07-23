using System;

namespace SqlBulkCopyTool
{
    public class Logger
    {
        public void Log(string message) => Console.WriteLine(message);
        public void LogError(string message) => WriteInColor(message, ConsoleColor.Red);
        public void LogSuccess(string message) => WriteInColor(message, ConsoleColor.Green);

        private void WriteInColor(string message, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ResetColor();
        }
    }
}

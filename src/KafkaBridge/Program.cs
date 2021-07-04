using System;
using System.CommandLine;
using System.Threading.Tasks;
using KafkaBridge.Commands;
using KafkaBridge.Services;

namespace KafkaBridge
{
    class Program
    {
        /// <summary>
        /// Starts kafka bridge service.
        /// </summary>
        /// <param name="args">Console parameters</param>
        static async Task Main(string[] args)
        {
            var cmd = BuildRootCommand();

            await cmd.InvokeAsync(args);

            Console.WriteLine("Press any key to stop");
            Console.ReadKey();
        }

        private static RootCommand BuildRootCommand()
        {
            var cmd = new RootCommand();
            var kafkaService = new KafkaService();
            var copyCommand = new CopyCommand(kafkaService);
            cmd.AddCommand(copyCommand);

            var consumeCommand = new ConsumeCommand(kafkaService);
            cmd.AddCommand(consumeCommand);

            return cmd;
        }
    }
}
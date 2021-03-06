﻿using System.Web.Script.Serialization;

namespace AmazonSqs.Tests {
    public class TestObject {
        public int Id { get; set; }
        public string Name { get; set; }

        public TestObject NestedObject { get; set; }

        [ScriptIgnore]
        public string IgnoredProperty { get; set; }
    }
}

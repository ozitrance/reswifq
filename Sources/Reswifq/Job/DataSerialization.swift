//
//  DataSerialization.swift
//  Reswifq
//
//  Created by Valerio Mazzeo on 21/02/2017.
//  Copyright © 2017 VMLabs Limited. All rights reserved.
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//  See the GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program. If not, see <http://www.gnu.org/licenses/>.
//

import Foundation

// MARK: - DataEncodable

public protocol DataEncodable {

    func data() throws -> Data
}

public enum DataEncodableError: Error {
    case cannotEncodeType(Any)
}

// MARK: - DataDecodable

public protocol DataDecodable {

    init(data: Data) throws
}

public enum DataDecodableError: Error, Equatable {

    case invalidData(Data)

    // MARK: Equatable

    public static func == (lhs: DataDecodableError, rhs: DataDecodableError) -> Bool {

        switch (lhs, rhs) {
        case (let .invalidData(lhs), let .invalidData(rhs)):
            return lhs == rhs
        }
    }
}
